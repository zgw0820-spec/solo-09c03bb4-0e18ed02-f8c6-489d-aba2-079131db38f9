#!/usr/bin/env python3
"""Live HTTP trial against a running stack (scripts/run-local.sh or compose).

Covers:
  A. in-flight publish (two landings, adjustable interval) + drain + success bundle
  B. capture timeout (>3s agent latency) -> no download, resume -> late report
     from round 1 cannot change its verdict or interfere with round 2
  C. injected one-shot agent failure -> immediate failure + resume

Stdlib only.  Usage: scripts/trial.py [web_base_url]
"""
from __future__ import annotations

import io
import json
import sys
import time
import urllib.error
import urllib.request
import zipfile

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
PASS = "✅"
FAIL = "❌"


def call(method: str, path: str, body=None, base: str = BASE):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        base + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
            ctype = resp.headers.get("Content-Type", "")
            if "application/json" in ctype:
                return resp.status, (json.loads(raw) if raw else None), raw
            return resp.status, None, raw
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        try:
            return exc.code, json.loads(raw), raw
        except Exception:
            return exc.code, None, raw


def wait_round(rid: str, timeout: float = 20.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        st, d, _ = call("GET", f"/api/rounds/{rid}")
        r = d["round"]
        if r["terminal"]:
            return r
        time.sleep(0.1)
    raise AssertionError(f"round {rid} did not finish in {timeout}s")


def wait_download(rid: str, timeout: float = 5.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        st, d, _ = call("GET", f"/api/rounds/{rid}")
        if d["round"]["download_available"]:
            return True
        time.sleep(0.1)
    return False


def check(name: str, ok: bool, detail: str = ""):
    print(f"  {PASS if ok else FAIL} {name}" + (f" — {detail}" if detail else ""))
    if not ok:
        check.failed += 1


check.failed = 0


def scenario_a():
    print("\n=== 试跑 A：在途发布 → 等写齐两卷 → 双卷成功 → 整组下载 ===")
    call("PUT", "/api/agents/index/config", {"delay_seconds": 0.2, "fail_mode": "none"})
    call("PUT", "/api/agents/blobs/config", {"delay_seconds": 0.2, "fail_mode": "none"})

    st, d, _ = call("POST", "/api/artifacts",
                    {"name": "试跑A-在途制品", "interval_seconds": 2.0})
    aid = d["artifact"]["id"]
    check("发布被接单 (201)", st == 201, aid)
    time.sleep(0.3)  # let the first (blob) landing happen, interval still pending

    t0 = time.monotonic()
    st, d, _ = call("POST", "/api/backup/start")
    rid = d["round"]["id"]
    check("备份轮次启动 (201)", st == 201, rid)

    st2, d2, _ = call("POST", "/api/backup/start")
    check("并发第二轮被拒 (409, 一次只跑一轮)", st2 == 409
          and d2["error_code"] == "round_already_active", f"status={st2}")

    st3, d3, _ = call("POST", "/api/artifacts", {"name": "备份期间"})
    check("暂停接单期间发布被拒 (409)", st3 == 409
          and d3["error_code"] == "acceptance_paused", f"status={st3}")

    r = wait_round(rid)
    wall = time.monotonic() - t0
    check("轮次判定成功", r["state"] == "success", r["state"])
    # 0.3s of the 2.0s interval elapsed before backup started.
    check("确实先排空在途写入 (~1.7s)", r["drain_seconds"] >= 1.55,
          f"drain={r['drain_seconds']:.2f}s wall={wall:.2f}s")
    check("两卷都按时回报成功",
          all(x.get("ok") for x in r["reports"].values()),
          ", ".join(f"{k}:{x.get('ok')}" for k, x in r["reports"].items()))

    st, s, _ = call("GET", "/api/state")
    check("成功后立即恢复接单", s["accepting"] is True and s["active_round_id"] is None)

    art = next(a for a in s["artifacts"] if a["id"] == aid)
    gap = art["index_at_unix"] - art["blob_at_unix"]
    check("两次落盘间隔可调且真实生效 (~2.0s)", gap >= 1.9, f"gap={gap:.2f}s")

    check("整组 zip 变为可下载", wait_download(rid))
    st, _, raw = call("GET", f"/api/rounds/{rid}/download")
    check("下载返回 zip (200)", st == 200 and raw[:2] == b"PK", f"{len(raw)} bytes")
    zf = zipfile.ZipFile(io.BytesIO(raw))
    names = zf.namelist()
    man = json.loads(zf.read("manifest.json"))
    check("zip 含 manifest 且轮次号一致", man["round_id"] == rid)
    check("zip 同时含 index/ 与 blobs/ 同轮快照",
          any(n.startswith("index/") for n in names)
          and any(n.startswith("blobs/") for n in names),
          f"{len(names)} entries")
    print(f"  ℹ zip entries: {names[:6]}{' ...' if len(names) > 6 else ''}")


def scenario_b():
    print("\n=== 试跑 B：抓取超时（索引卷延迟 4.5s > 3s 窗口）+ 迟到回报隔离 ===")
    call("PUT", "/api/agents/index/config", {"delay_seconds": 4.5, "fail_mode": "none"})
    call("PUT", "/api/agents/blobs/config", {"delay_seconds": 0.2, "fail_mode": "none"})

    t0 = time.monotonic()
    st, d, _ = call("POST", "/api/backup/start")
    rid1 = d["round"]["id"]
    r1 = wait_round(rid1, timeout=8)
    elapsed = time.monotonic() - t0
    check("首轮在 ~3s 窗口处判超时失败",
          r1["state"] == "failed" and r1["failure_code"] == "capture_timeout",
          f"{r1['state']}/{r1.get('failure_code')} after {elapsed:.2f}s")
    check("判定耗时被 3s 窗口卡住 (<3.6s)", elapsed < 3.6, f"{elapsed:.2f}s")

    st, s, _ = call("GET", "/api/state")
    check("超时后立即恢复接单", s["accepting"] is True)
    st, _, _ = call("GET", f"/api/rounds/{rid1}/download")
    check("失败轮不提供下载 (409)", st == 409, f"status={st}")

    # Heal and immediately run the next round (while round-1 index call sleeps).
    call("PUT", "/api/agents/index/config", {"delay_seconds": 0.15})
    st, d, _ = call("POST", "/api/backup/start")
    check("恢复后可立即发起下一轮 (201)", st == 201)
    rid2 = d["round"]["id"]
    r2 = wait_round(rid2)
    check("第二轮成功", r2["state"] == "success", r2["state"])
    check("第二轮整组可下载", wait_download(rid2))

    # Wait for round 1's late report (4.5s agent latency).
    late = None
    end = time.monotonic() + 6
    while time.monotonic() < end:
        _, dd, _ = call("GET", f"/api/rounds/{rid1}")
        if dd["round"]["late_report_count"] >= 1:
            late = dd["round"]
            break
        time.sleep(0.2)
    check("首轮收到迟到回报并归档", late is not None)
    if late:
        lr = late["late_reports"][0]
        check("迟到的是索引卷且实际成功", lr["label"] == "index" and lr["report"]["ok"] is True)
        check("迟到回报未改动原结果（仍 timeout 失败）",
              late["state"] == "failed" and late["failure_code"] == "capture_timeout")

    _, d2, _ = call("GET", f"/api/rounds/{rid2}")
    r2f = d2["round"]
    check("下一轮没有被迟到回报干扰（成功且无迟到记录）",
          r2f["state"] == "success" and r2f["late_report_count"] == 0
          and all(x.get("ok") for x in r2f["reports"].values()))


def scenario_c():
    print("\n=== 试跑 C：快照端注入一次性失败 → 立即失败恢复；慢卷回报转迟到 ===")
    call("PUT", "/api/agents/index/config", {"delay_seconds": 1.2, "fail_mode": "none"})
    call("PUT", "/api/agents/blobs/config", {"delay_seconds": 0.1, "fail_mode": "none"})
    call("PUT", "/api/agents/blobs/config", {"fail_once": True})

    t0 = time.monotonic()
    st, d, _ = call("POST", "/api/backup/start")
    rid = d["round"]["id"]
    r = wait_round(rid)
    elapsed = time.monotonic() - t0
    check("任一卷失败立即判定 agent_failure",
          r["state"] == "failed" and r["failure_code"] == "agent_failure",
          f"after {elapsed:.2f}s")
    check("没有干等慢卷/窗口 (<0.7s)", elapsed < 0.7, f"{elapsed:.2f}s")
    st, s, _ = call("GET", "/api/state")
    check("失败后立即恢复接单", s["accepting"] is True)
    st, _, _ = call("GET", f"/api/rounds/{rid}/download")
    check("该轮不提供下载 (409)", st == 409)

    end = time.monotonic() + 4
    late = None
    while time.monotonic() < end:
        _, dd, _ = call("GET", f"/api/rounds/{rid}")
        if dd["round"]["late_report_count"] >= 1:
            late = dd["round"]
            break
        time.sleep(0.15)
    check("慢索引卷随后的成功回报作为迟到归档，不改结果",
          late is not None and late["state"] == "failed"
          and any(x["label"] == "index" and x["report"]["ok"] for x in late["late_reports"]))


if __name__ == "__main__":
    print(f"针对 {BASE} 执行实况试跑")
    try:
        scenario_a()
        scenario_b()
        scenario_c()
    except Exception as exc:
        print(f"{FAIL} 试跑脚本异常: {type(exc).__name__}: {exc}")
        raise
    print(f"\n结果：{ '全部通过' if check.failed == 0 else str(check.failed) + ' 项失败'}")
    sys.exit(1 if check.failed else 0)
