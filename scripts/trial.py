#!/usr/bin/env python3
"""Live HTTP trial against a running stack (scripts/run-local.sh or compose).

Covers:
  A. in-flight publish (two landings, adjustable interval) + drain + success bundle
  B. capture timeout (>3s agent latency) -> no download, resume -> late report
     from round 1 cannot change its verdict or interfere with round 2
  C. injected one-shot agent failure -> immediate failure + resume
  D. queued publish while paused: nothing written, withdraw never publishes,
     valid reservations accepted in order on resume, next round drains them
  E. a reservation that sits past its 10s TTL auto-expires and is never published

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
RUN = str(int(time.time()))[-5:]  # unique suffixes: repeat runs must not collide
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


def wait_reservation(rid: str, state: str, timeout: float = 15.0):
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        _, s, _ = call("GET", "/api/state")
        last = next((r for r in s["reservations"] if r["id"] == rid), None)
        if last and last["state"] == state:
            return last
        time.sleep(0.1)
    return last


def scenario_d():
    print("\n=== 试跑 D：暂停期间排队发布 → 撤回单不发布 → 恢复接单按序接下 → 下一轮先排空 ===")
    call("PUT", "/api/agents/index/config", {"delay_seconds": 0.2, "fail_mode": "none"})
    call("PUT", "/api/agents/blobs/config", {"delay_seconds": 0.2, "fail_mode": "none"})

    st, d, _ = call("POST", "/api/artifacts", {"name": "D-在途-" + RUN, "interval_seconds": 2.0})
    inflight_id = d["artifact"]["id"]
    time.sleep(0.2)
    st, d, _ = call("POST", "/api/backup/start")
    rid1 = d["round"]["id"]
    check("备份开始、接单暂停", st == 201)

    st, d, _ = call("POST", "/api/artifacts", {"name": "D-直接发布-" + RUN})
    check("暂停期间原发布接口仍 409", st == 409 and d["error_code"] == "acceptance_paused",
          f"status={st}")

    st, q1, _ = call("POST", "/api/publish-queue", {"name": "D-预约甲-" + RUN, "interval_seconds": 0.5})
    check("排队发布甲 (201/queued)", st == 201 and q1["reservation"]["state"] == "queued")
    st, q2, _ = call("POST", "/api/publish-queue", {"name": "D-预约乙-" + RUN, "interval_seconds": 0.5})
    check("排队发布乙 (201/queued)", st == 201 and q2["reservation"]["state"] == "queued")
    r1, r2 = q1["reservation"]["id"], q2["reservation"]["id"]

    _, s, _ = call("GET", "/api/state")
    names = {a["name"] for a in s["artifacts"]}
    check("排队期间不写盘（未出现制品、无 artifact 记录）",
          "D-预约甲-" + RUN not in names and "D-预约乙-" + RUN not in names)
    print(f"  ℹ 排队中数: {s['queued_reservations']}")
    check("排队期间两单均无对应制品记录",
          not any(a["reservation_id"] in (r1, r2) for a in s["artifacts"]))

    st, d, _ = call("POST", f"/api/publish-queue/{r2}/withdraw")
    check("撤回乙单成功", st == 200 and d["reservation"]["state"] == "withdrawn")
    st, d, _ = call("POST", f"/api/publish-queue/{r2}/withdraw")
    check("重复撤回被拒 (409)", st == 409 and d["error_code"] == "reservation_not_queued")

    rr = wait_round(rid1)
    check("首轮成功结束", rr["state"] == "success", rr["state"])
    _, s, _ = call("GET", "/api/state")
    by_id = {r["id"]: r for r in s["reservations"]}
    check("恢复接单后甲单已接单", by_id[r1]["state"] == "accepted"
          and by_id[r1]["artifact_id"], str(by_id[r1]["state"]))
    check("乙单保持已撤回、未产生制品", by_id[r2]["state"] == "withdrawn"
          and by_id[r2]["artifact_id"] is None)
    names = {a["name"] for a in s["artifacts"]}
    check("乙单从未发布", "D-预约乙-" + RUN not in names)
    check("甲单制品已出现", "D-预约甲-" + RUN in names)

    # Immediately start the next round: it must wait for the accepted order.
    t0 = time.monotonic()
    st, d, _ = call("POST", "/api/backup/start")
    rid2 = d["round"]["id"] if d and "round" in d else None
    check("恢复后立刻发起下一轮 (201)", st == 201, str(rid2))
    rr2 = wait_round(rid2)
    check("第二轮成功", rr2["state"] == "success", rr2["state"])
    check("第二轮把甲单算作暂停时在途并先排空",
          by_id[r1]["artifact_id"] in rr2["inflight_at_pause"]
          and rr2["drain_seconds"] > 0.05,
          f"drain={rr2['drain_seconds']:.2f}s inflight={len(rr2['inflight_at_pause'])} "
          f"wall={time.monotonic() - t0:.2f}s")
    _, s, _ = call("GET", "/api/state")
    art = next(a for a in s["artifacts"] if a["id"] == by_id[r1]["artifact_id"])
    check("甲单制品的两卷都已写齐再被抓取", art["state"] == "complete"
          and art["blob_at_unix"] and art["index_at_unix"])


def scenario_e():
    print("\n=== 试跑 E：预约满 10 秒仍未接单 → 自动作废 → 恢复接单也不发布 ===")
    call("PUT", "/api/agents/index/config", {"delay_seconds": 0.2, "fail_mode": "none"})
    call("PUT", "/api/agents/blobs/config", {"delay_seconds": 0.2, "fail_mode": "none"})

    # A 14s in-flight order keeps acceptance paused well past the 10s TTL.
    call("POST", "/api/artifacts", {"name": "E-慢在途-" + RUN, "interval_seconds": 14})
    time.sleep(0.2)
    st, d, _ = call("POST", "/api/backup/start")
    rid = d["round"]["id"]
    st, q, _ = call("POST", "/api/publish-queue", {"name": "E-等到作废-" + RUN, "interval_seconds": 0})
    rsv_id = q["reservation"]["id"]
    t0 = time.monotonic()
    expired = wait_reservation(rsv_id, "expired", timeout=13)
    elapsed = time.monotonic() - t0
    check("预约在约 10s 时自动作废", expired is not None and expired["state"] == "expired",
          f"{expired['state'] if expired else None} after {elapsed:.2f}s")
    check("作废耗时接近 10s 上限 (<11.5s)", 9.5 <= elapsed < 11.5, f"{elapsed:.2f}s")
    st, d, _ = call("POST", f"/api/publish-queue/{rsv_id}/withdraw")
    check("已作废的预约不能撤回 (409)", st == 409 and d.get("state") == "expired")

    rr = wait_round(rid, timeout=30)
    check("慢在途制品写齐后本轮成功", rr["state"] == "success", rr["state"])
    _, s, _ = call("GET", "/api/state")
    names = {a["name"] for a in s["artifacts"]}
    by_id = {r["id"]: r for r in s["reservations"]}
    check("作废预约始终未发布", "E-等到作废-" + RUN not in names
          and by_id[rsv_id]["state"] == "expired")
    check("同轮其它排队均未残留", s["queued_reservations"] == 0)


if __name__ == "__main__":
    print(f"针对 {BASE} 执行实况试跑")
    try:
        scenario_a()
        scenario_b()
        scenario_c()
        scenario_d()
        scenario_e()
    except Exception as exc:
        print(f"{FAIL} 试跑脚本异常: {type(exc).__name__}: {exc}")
        raise
    print(f"\n结果：{ '全部通过' if check.failed == 0 else str(check.failed) + ' 项失败'}")
    sys.exit(1 if check.failed else 0)
