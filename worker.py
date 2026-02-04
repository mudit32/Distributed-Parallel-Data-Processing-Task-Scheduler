# worker.py
import requests
import time
import sys
import uuid
import threading
import random
from typing import Any, List

MASTER_URL = "http://10.194.160.251:8000"  # update as needed
WORKER_ID = sys.argv[1] if len(sys.argv) > 1 else f"worker-{uuid.uuid4().hex[:6]}"

# Random small startup delay so many workers don't all hit master at once
time.sleep(random.uniform(0, 2))

def heartbeat_loop():
    while True:
        try:
            requests.post(
                f"{MASTER_URL}/heartbeat",
                json={"worker_id": WORKER_ID, "timestamp": time.time()},
                timeout=2
            )
        except Exception:
            pass
        time.sleep(5)

def do_sort(arr: List[Any]):
    try:
        return sorted(arr)
    except Exception as e:
        return str(e)

def do_matmul(A: List[List[float]], B: List[List[float]]):
    try:
        n, m, p = len(A), len(B), len(B[0])
        C = [[0]*p for _ in range(n)]
        for i in range(n):
            for j in range(p):
                s = 0
                for k in range(m):
                    s += A[i][k] * B[k][j]
                C[i][j] = s
        return C
    except Exception as e:
        return str(e)

def do_math(expr: str):
    try:
        # simple evaluation: evaluate expression and return value
        # WARNING: eval is potentially unsafe if you accept untrusted input.
        return eval(expr)
    except Exception as e:
        return str(e)

def task_loop():
    while True:
        try:
            r = requests.get(f"{MASTER_URL}/get_task", params={"worker_id": WORKER_ID}, timeout=5)
            r.raise_for_status()
            task = r.json().get("task")
            if not task:
                time.sleep(random.uniform(0.5, 1.5))
                continue

            tid = task["task_id"]
            ttype = task["type"]
            payload = task.get("payload", {})
            print(f"[{WORKER_ID}] got task {tid} ({ttype}) -> {payload}")

            if ttype == "sort":
                result = {"sorted": do_sort(payload.get("array", []))}
            elif ttype == "matmul":
                result = {"C": do_matmul(payload.get("A", []), payload.get("B", []))}
            elif ttype == "math":
                expr = payload.get("expr", "")
                result = {"value": do_math(expr)}
            else:
                result = {"error": "unknown type"}

            # Simulate processing delay
            time.sleep(random.uniform(1, 3))

            # Submit result
            try:
                requests.post(
                    f"{MASTER_URL}/submit_result",
                    json={
                        "task_id": tid,
                        "status": "done",
                        "result": result,
                        "worker_id": WORKER_ID
                    },
                    timeout=5
                )
                print(f"[{WORKER_ID}] finished {tid}")
            except Exception as e:
                print(f"[{WORKER_ID}] failed to submit result for {tid}: {e}")

        except Exception as e:
            print(f"[{WORKER_ID}] error: {e}")
            time.sleep(random.uniform(1, 2))

if __name__ == "__main__":
    print(f"🚀 Starting worker {WORKER_ID}")
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    task_loop()