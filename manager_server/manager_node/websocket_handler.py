# - manager_node/websocket_handler.py -

# This script to create a websocket between manager node and worker node for checking task status during a interval
# Manager Node Websocket Server

import asyncio
import json
import logging
from datetime import datetime, timedelta
from aiohttp import web

# 全局状态
workers_status = {}
WORKERS_LOCK = asyncio.Lock()

# 日志
stdout_logger = logging.getLogger('stdout_logger')
chronograph_logger = logging.getLogger('chronograph_logger')


async def websocket_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    async for msg in ws:
        if msg.type == web.WSMsgType.TEXT:
            try:
                data = json.loads(msg.data)
                action = data.get('action')

                if action == 'register':
                    worker_id = data.get('worker_id')
                    async with WORKERS_LOCK:
                        workers_status[worker_id] = {
                            'last_heartbeat': datetime.now(datetime.timezone.utc),
                            'wait_time': data.get('wait_time', 0.0),
                            'processing_cnt': data.get('processing_cnt', 0),
                            'websocket': ws
                        }
                    stdout_logger.info(f"Worker {worker_id} registered.")
                    await ws.send_str(json.dumps({'status': 'registered'}))

                elif action == 'heartbeat':
                    worker_id = data.get('worker_id')
                    async with WORKERS_LOCK:
                        if worker_id in workers_status:
                            workers_status[worker_id]['last_heartbeat'] = datetime.utcnow()
                            workers_status[worker_id]['wait_time'] = data.get('wait_time', workers_status[worker_id]['wait_time'])
                            workers_status[worker_id]['processing_cnt'] = data.get('processing_cnt', workers_status[worker_id]['processing_cnt'])
                            chronograph_logger.info(f"Heartbeat received from {worker_id}: {workers_status[worker_id]}")
                        else:
                            stdout_logger.warning(f"Unknown worker {worker_id} sent heartbeat.")

                elif action == 'task_update':
                    # 处理任务更新（根据需要实现）
                    pass

                else:
                    stdout_logger.warning(f"Unknown action received: {action}")

            except json.JSONDecodeError:
                stdout_logger.error("Failed to decode JSON message.")
            except Exception as e:
                stdout_logger.error(f"Error processing message: {e}")

        elif msg.type == web.WSMsgType.ERROR:
            stdout_logger.error(f"WebSocket connection closed with exception {ws.exception()}")

    # 连接关闭时清理状态
    async with WORKERS_LOCK:
        for worker_id, status in list(workers_status.items()):
            if status.get('websocket') == ws:
                del workers_status[worker_id]
                stdout_logger.info(f"Worker {worker_id} disconnected and removed from active workers.")
                break

    stdout_logger.info("WebSocket connection closed.")
    return ws

async def assign_task_to_worker(worker_id, task_data):
    try:
        async with WORKERS_LOCK:
            worker_ws = workers_status.get(worker_id, {}).get('websocket')
        if worker_ws:
            assign_message = {
                'action': 'assign_task',
                'task': task_data
            }
            await worker_ws.send_str(json.dumps(assign_message))
            stdout_logger.info(f"Assigned task {task_data.get('task_id')} to worker {worker_id}")
        else:
            stdout_logger.warning(f"No WebSocket connection found for worker {worker_id}")
    except Exception as e:
        stdout_logger.error(f"Failed to assign task to worker {worker_id}: {e}")

async def check_heartbeats():
    while True:
        async with WORKERS_LOCK:
            now = datetime.utcnow()
            to_remove = []
            for worker_id, status in workers_status.items():
                if now - status['last_heartbeat'] > timedelta(seconds=10):
                    stdout_logger.warning(f"Worker {worker_id} missed heartbeat. Removing from active workers.")
                    to_remove.append(worker_id)
            for worker_id in to_remove:
                del workers_status[worker_id]
        await asyncio.sleep(5)