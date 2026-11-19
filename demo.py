"""Bounded two-process simulation; both robots talk through the real DingTalk group."""
import json
import queue
import subprocess
import sys
import threading

from peer import ROOT, load_config, log


def demo():
    config = load_config(ROOT / "local.json")
    children, ready, completed = [], set(), set()
    output = queue.Queue()
    runtime = ROOT / "runtime"
    runtime.mkdir(exist_ok=True)

    def pump(child, peer):
        for line in child.stdout:
            output.put((peer, line))
        output.put((peer, None))

    def launch(peer):
        child = subprocess.Popen([sys.executable, str(ROOT / "peer.py"), "--peer", peer, "run", "--duration", "120s"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="replace")
        children.append(child)
        threading.Thread(target=pump, args=(child, peer), daemon=True).start()
    launch("A")
    started, task_id, live = False, None, {"A"}
    with open(runtime / "last-demo.jsonl", "w", encoding="utf-8") as transcript:
        while live:
            peer, line = output.get()
            if line is None:
                live.discard(peer)
                continue
            print(line, end="", flush=True)
            transcript.write(line)
            transcript.flush()
            try:
                event = json.loads(line)
            except ValueError:
                continue
            if event.get("event") == "ready":
                ready.add(peer)
                if peer == "A" and "B" not in live:
                    launch("B")
                    live.add("B")
            if event.get("event") == "task_completed" and event.get("task") == task_id:
                completed.add(peer)
            if ready == {"A", "B"} and not started:
                started = True
                goal = "仅做P2P通信模拟，不涉及真实业务或设备。B先回复模拟接口 /p2p/ping 的请求字段 traceId，并将示例值 P2P-20261023-01 原样回报；A收到后核对字段和值，给出通信验证结论并结束任务。B首轮必须reply给A，A核对后使用finish。"
                result = subprocess.run([sys.executable, str(ROOT / "peer.py"), "--peer", "A", "start", "--to", "B", "--goal", goal], capture_output=True, text=True, encoding="utf-8", timeout=60)
                print(result.stdout, end="", flush=True)
                transcript.write(result.stdout)
                if result.returncode:
                    log("demo_start_failed")
                else:
                    task_id = json.loads(result.stdout.strip())["task"]
    exit_codes = [p.wait() for p in children]
    success = completed == {"A", "B"} and exit_codes == [0, 0]
    report = {"success": success, "task": task_id, "ready": sorted(ready), "completed": sorted(completed), "exit_codes": exit_codes, "scope": "same-computer simulation; no business program was invoked"}
    (runtime / "last-demo-result.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    log("demo_result", **report)
    return 0 if success else 1


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.exit(demo())
