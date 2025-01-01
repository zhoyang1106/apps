# manager_node/main.py
import asyncio
from pathlib import Path
from aiohttp import web, ClientSession
from utils import setup_logger, load_xgboost_model
import traceback
import time
import xgboost as xgb

WORKERS= [
    'http://192.168.0.150',
    'http://192.168.0.151',
    'http://192.168.0.152',
]


def predict_processed_time(request_data: dict):
    data = xgb.DMatrix([[request_data.get('number')]])
    prediction = xgboost_model.predict(data)
    return float(prediction[0])


def setup_loggers():
    PARENT_DIR = Path(__file__).parent.parent
    stdout_logger = setup_logger('stdout_logger', PARENT_DIR / 'logs' / 'manager_stdout.log')
    diff_graph_logger = setup_logger('diff_graph_logger', PARENT_DIR / 'logs' / 'diff_graph.log')
    chrono_logger = setup_logger('chrono_logger', PARENT_DIR / 'logs' / 'chrono_graph.log')
    print("logger started")
    return stdout_logger, diff_graph_logger, chrono_logger



def select_worker(app, algorithm_name):
    if algorithm_name == 'shortest':
        selected_worker = min(app['wait_times'], key=app['wait_times'].get)
        return selected_worker, WORKERS[selected_worker]
    elif algorithm_name == 'round-robin':
        selected_worker = WORKERS[app['selected_worker_index']]
        app['selected_worker_index'] += 1
        return selected_worker
    else:
        return None, None


# timer task
async def countdown_task(app, worker, interval=0.1):
    try:
        while True:
            async with app['locks'][worker]:
                if app['wait_times'][worker] > 0:
                    app['wait_times'][worker] -= interval
                    if app['wait_times'][worker] < 0:
                        app['wait_times'][worker] = 0
                else:
                    app['wait_times'][worker] = 0
                    
            await asyncio.sleep(interval)            
    except asyncio.CancelledError:
        stdout_logger.info(f"Backend {worker} countdown task canceled.")

# start
async def on_startup(app):
    app['wait_times'] = {i: 0 for i in range(len(WORKERS))}
    app['locks'] = {i: asyncio.Lock() for i in range(len(WORKERS))}
    app['countdown_tasks'] = [
        asyncio.create_task(countdown_task(app, i, interval=0.1))
        for i in range(len(WORKERS))
    ]
    app['sessions'] = {
        i: ClientSession() for i in range(len(WORKERS))
    }

    print("All countdown tasks and client sessions started.")

async def handle(request: web.Request):
    receive = time.time()

    try:
        request_data: dict = await request.json()
        stdout_logger.info(request_data)
        request_id = request_data.get('request_id', None)

        worker_id, worker_url = select_worker(request.app, request_data['algorithm_name'])

        task_predict_process_time = predict_processed_time()

        async with request.app['locks'][worker_id]:
            task_wait_time = request.app['wait_times'][worker_id]
            request.app['wait_times'][worker_id] += task_predict_process_time
            stdout_logger.info(f"Reqeust {request_id} to backend {worker_id}, add wait time {task_predict_process_time} s, all wait time {request.app['wait_times'][worker_id]} s")

        # request to backend
        async with request.app['sessions'][worker_id].post(worker_url, json=request_data) as resp:
            if resp.status != 200:
                raise Exception(f"Backend {worker_id} return status {resp.status}")

            data: dict = await resp.json()
            status = data.get("status", None)
            task_start_time = data.get('start', receive)
            task_real_process_time = data.get('real_process_time')

            stdout_logger.info(f"Request {request_id} on backend {worker_id} process time {task_real_process_time} s, predict wait time {task_wait_time} s")

            response = {
                "status": status,
                "calculate_wait_time": task_wait_time,
                "real_wait_time": task_start_time - receive
            }

            return web.json_response(response)
    except Exception as e:
        err_msg = traceback.format_exc()
        stdout_logger.error(f"Error in request handler: {err_msg}")
        return web.json_response({"error": "Failed to process request"}, status=500)


async def on_cleanup(app):
    for task in app['countdown_tasks']:
        task.cancel()
    await asyncio.gather(*app['countdown_tasks'], return_exceptions=True)
    for session in app['sessions'].values():
        await session.close()
    print("All countdown tasks and client sessions stopped.")


# setup loggers
stdout_logger, diff_graph_logger, chrono_logger = setup_loggers()

# xgboost model start
xgboost_model = load_xgboost_model()

def create_app():
    app = web.Application()
    app.router.add_post('', handle)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app

if __name__ == '__main__':
    web.run_app(create_app(), host='192.168.0.100', port=8199)
