# worker_node/websocket_client.py
import asyncio
import websockets
import json
import logging
from pathlib import Path

def setup_logger(name, log_path):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(log_path, mode='a')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

worker_logger = setup_logger('worker_logger', Path(__file__).parent.parent / 'logs' / 'worker.log')

class WorkerNode:
    def __init__(self, worker_id, manager_ws_uri, initial_wait_time=0.0):
        self.worker_id = worker_id
        self.manager_ws_uri = manager_ws_uri
        self.wait_time = initial_wait_time
        self.processing_cnt = 0
        self.websocket = None
        self.logger = worker_logger

    async def connect(self):
        while True:
            try:
                self.websocket = await websockets.connect(self.manager_ws_uri)
                await self.register()
                asyncio.create_task(self.send_heartbeat())
                asyncio.create_task(self.listen())
                break
            except Exception as e:
                self.logger.error(f"Failed to connect to manager: {e}")
                await asyncio.sleep(5)

    async def register(self):
        register_message = {
            'action': 'register',
            'worker_id': self.worker_id,
            'wait_time': self.wait_time,
            'processing_cnt': self.processing_cnt
        }
        await self.websocket.send(json.dumps(register_message))
        response = await self.websocket.recv()
        response_data = json.loads(response)
        if response_data.get('status') == 'registered':
            self.logger.info("Successfully registered with manager.")
        else:
            self.logger.warning("Registration failed.")

    async def send_heartbeat(self):
        while True:
            try:
                heartbeat_message = {
                    'action': 'heartbeat',
                    'worker_id': self.worker_id,
                    'wait_time': self.wait_time,
                    'processing_cnt': self.processing_cnt
                }
                await self.websocket.send(json.dumps(heartbeat_message))
                self.logger.info("Heartbeat sent.")
            except Exception as e:
                self.logger.error(f"Failed to send heartbeat: {e}")
                await self.connect()
            await asyncio.sleep(5)  # 每5秒发送一次心跳

    async def listen(self):
        try:
            async for message in self.websocket:
                data = json.loads(message)
                action = data.get('action')
                if action == 'assign_task':
                    task_data = data.get('task')
                    asyncio.create_task(self.handle_task(task_data))
        except websockets.ConnectionClosed:
            self.logger.warning("Connection to manager closed. Reconnecting...")
            await self.connect()
        except Exception as e:
            self.logger.error(f"Error in listen: {e}")
            await self.connect()

    async def handle_task(self, task_data):
        task_id = task_data.get('task_id')
        self.logger.info(f"Received task {task_id}: {task_data}")
        self.processing_cnt += 1
        self.wait_time += task_data.get('pred_processed_time', 0.0)

        # 模拟任务处理
        await asyncio.sleep(task_data.get('pred_processed_time', 0.0))

        self.processing_cnt -= 1
        self.wait_time -= task_data.get('pred_processed_time', 0.0)
        self.logger.info(f"Completed task {task_id}")

        # 发送任务完成更新
        await self.send_task_update(task_id)

    async def send_task_update(self, task_id):
        task_update_message = {
            'action': 'task_update',
            'worker_id': self.worker_id,
            'task_id': task_id,
            'status': 'completed'
            # 添加其他需要的信息
        }
        try:
            await self.websocket.send(json.dumps(task_update_message))
            self.logger.info(f"Task update sent for task {task_id}.")
        except Exception as e:
            self.logger.error(f"Failed to send task update: {e}")
