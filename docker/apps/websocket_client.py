# worker_node/websocket_client.py
import asyncio
import websockets
import json
import logging
from pathlib import Path



def setup_logger(name, log_path: Path):
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

    
