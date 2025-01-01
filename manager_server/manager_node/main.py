# manager_node/main.py
import asyncio
from pathlib import Path
from aiohttp import web
from utils import setup_logger, load_xgboost_model
import traceback


async_model = False

if async_model:
    from http_handler_async import ManagerNode, Worker
    print("Import http_handler_async.py")
else:
    from http_handler import ManagerNode, Worker
    print("Import http_handler.py")



def setup_loggers():
    PARENT_DIR = Path(__file__).parent.parent
    stdout_logger = setup_logger('stdout_logger', PARENT_DIR / 'logs' / 'manager_stdout.log')
    diff_graph_logger = setup_logger('diff_graph_logger', PARENT_DIR / 'logs' / 'diff_graph.log')
    chrono_logger = setup_logger('chrono_logger', PARENT_DIR / 'logs' / 'chrono_graph.log')
    print("logger started")
    return stdout_logger, diff_graph_logger, chrono_logger



async def init_app():

    # 创建 aiohttp 应用
    app = web.Application()

    # 初始化日志
    stdout_logger, diff_graph_logger, chrono_logger = setup_loggers()

    # 加载 xgboost 模型
    xgboost_model = load_xgboost_model()

    # 初始化 ManagerNode
    manager_node = ManagerNode(xgboost_model=xgboost_model, workers=[
        Worker(ip='192.168.0.150', port=8080, id='150', update_interval=1, cpu_limit=0.8),
        Worker(ip='192.168.0.151', port=8080, id='151', update_interval=1, cpu_limit=0.8),
        Worker(ip='192.168.0.152', port=8080, id='152', update_interval=1, cpu_limit=0.8),
    ], loggers=[stdout_logger, diff_graph_logger, chrono_logger])
    

    # app.router.add_get('/ws', websocket_handler)
    app.router.add_post("", manager_node.request_handler)

    
    # 启动后台任务
    app.on_startup.append(lambda app: manager_node.start_sessions())
    app.on_startup.append(lambda app: manager_node.start_worker_update_time_process())
    # app.on_startup.append(lambda app: manager_node.start_worker_hdd_mem_task())
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
