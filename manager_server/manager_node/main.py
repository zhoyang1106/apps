# manager_node/main.py
import asyncio
from pathlib import Path
from aiohttp import web
from websocket_handler import websocket_handler, check_heartbeats
from http_handler import ManagerNode, Worker
from utils import setup_logger, load_xgboost_model
import traceback


def setup_loggers():
    PARENT_DIR = Path(__file__).parent.parent
    stdout_logger = setup_logger('stdout_logger', PARENT_DIR / 'logs' / 'manager_stdout.log')
    chronograph_logger = setup_logger('chronograph_logger', PARENT_DIR / 'logs' / 'chronograph.log')

    print("logger started")
    return stdout_logger, chronograph_logger



async def init_app():

    # 创建 aiohttp 应用
    app = web.Application()

    # 初始化日志
    stdout_logger, chronograph_logger = setup_loggers()

    # 加载 xgboost 模型
    xgboost_model = load_xgboost_model()

    # 初始化 ManagerNode
    manager_node = ManagerNode(xgboost_model=xgboost_model, workers=[
        Worker(ip='192.168.0.150', port=8080, id='150'),
        Worker(ip='192.168.0.151', port=8080, id='151'),
        Worker(ip='192.168.0.152', port=8080, id='152'),
    ])
    

    app.router.add_get('/ws', websocket_handler)
    app.router.add_post("", manager_node.request_handler)

    
    # 启动后台任务
    app.on_startup.append(lambda app: manager_node.start_sessions())
    app.on_startup.append(lambda app: asyncio.create_task(check_heartbeats()))
    app.on_cleanup.append(manager_node.on_shutdown)

    return app

def run_manager():
    try:
        app = asyncio.run(init_app())
        web.run_app(app, host='0.0.0.0', port=8199)
    except Exception:
        print("End server")
        error_message = traceback.format_exc()
        print(error_message)

if __name__ == "__main__":
    run_manager()
