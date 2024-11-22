# worker_node/main.py
import asyncio
from .websocket_client import WorkerNode

def run_worker():
    worker_id = 'worker_1'  # 根据实际情况设置唯一的 worker_id
    manager_ws_uri = 'ws://192.168.0.100:8080/ws'  # 替换为管理节点的 WebSocket URI
    worker = WorkerNode(worker_id, manager_ws_uri)
    asyncio.run(worker.connect())

if __name__ == "__main__":
    run_worker()


async def main():
    global lock
    lock = asyncio.Lock()

    app = web.Application()
    app.router.add_post("", handle)

    # Run the web application
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()

    # Keep the application running
    while True:
        # Adjust as needed for your application's lifecycle
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())