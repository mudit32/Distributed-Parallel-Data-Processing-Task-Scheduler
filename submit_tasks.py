import requests

MASTER_URL = "http://127.0.0.1:8000"

tasks = [
    {"task_id": "task1", "type": "math", "payload": {"expr": "2 + 2"}},
    {"task_id": "task2", "type": "math", "payload": {"expr": "10 * 5"}},
    {"task_id": "task3", "type": "math", "payload": {"expr": "100 / 4"}},
    {"task_id": "task4", "type": "math", "payload": {"expr": "5 ** 3"}},
    {"task_id": "task5", "type": "math", "payload": {"expr": "7 * 8"}},
]

def submit_task(task):
    try:
        response = requests.post(f"{MASTER_URL}/submit_task", json=task)
        if response.status_code == 200:
            print(f"✅ Submitted {task['task_id']} successfully.")
        else:
            print(f"❌ Failed to submit {task['task_id']}: {response.status_code} {response.text}")
    except Exception as e:
        print(f"⚠️ Error submitting {task['task_id']}: {e}")

def main():
    print("🚀 Submitting tasks to master server...\n")
    for task in tasks:
        submit_task(task)
    print("\n✅ All tasks submitted.")

if __name__ == "__main__":
    main()
