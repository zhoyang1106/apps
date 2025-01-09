# manager_node/main.py
import asyncio
from pathlib import Path
from aiohttp import web, ClientSession, ClientTimeout
from utils import setup_logger, load_xgboost_model
import traceback
import time
import xgboost as xgb

WORKERS= [
    'http://192.168.0.150:8080',
    'http://192.168.0.151:8080',
    'http://192.168.0.152:8080',
]


ROUND_ROBIN_INDEX = 0

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
    global ROUND_ROBIN_INDEX
    if algorithm_name == 'shortest':
        selected_worker = min(app['wait_times'], key=app['wait_times'].get)
        return selected_worker, WORKERS[selected_worker]
    elif algorithm_name == 'round-robin':
        selected_worker = ROUND_ROBIN_INDEX
        ROUND_ROBIN_INDEX = (ROUND_ROBIN_INDEX + 1) % len(WORKERS)
        return selected_worker, WORKERS[selected_worker]
    elif algorithm_name == "min-entropy":
        selected_worker = max(app['dynamic_weight'], key=app['dynamic_weight'].get)
        return selected_worker, WORKERS[selected_worker]
    else:
        return 0, WORKERS[0]


# timer task
async def countdown_task(app, interval, worker_id):
    """Indecrease finish_times timestamp value"""
    try:
        while True:
            async with app['locks'][worker_id]:  
                new_wait_time = app['wait_times'][worker_id] - interval
                if new_wait_time < 0:
                    new_wait_time = 0
                app['wait_times'][worker_id] = new_wait_time
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        print("Countdown task was canceled")



# start tasks
async def on_startup(app):
    app['wait_times'] = {i: 0 for i in range(len(WORKERS))}
    app['locks'] = {i: asyncio.Lock() for i in range(len(WORKERS))}
    app['sessions'] = {
        i: ClientSession(timeout=ClientTimeout(None)) for i in range(len(WORKERS))
    }
    # app['countdown_tasks'] = {i: asyncio.create_task(countdown_task(app, interval=0.1, worker_id=i)) for i in range(len(WORKERS))}
    app['processing_tasks_sum'] = {i: 0 for i in range(len(WORKERS))}
    app['cumulative_times'] = {i: 0 for i in range(len(WORKERS))}
    app['dynamic_weight'] = {i: 1 / len(WORKERS) for i in range(len(WORKERS))}
    print("All countdown tasks and client sessions started.")


def update_weights(app):
    task_counts = list(app['processing_tasks_sum'].values())
    cumulative_times = list(app['cumulative_times'].values())
    weights = []
    for n, t in zip(task_counts, cumulative_times):
        weights.append(1 / (1 + n + t))

    total = sum(weights)
    for index, weight in enumerate(weights):
        app['dynamic_weight'][index] = weight / total

async def handle(request: web.Request):
    receive = time.time()

    try:
        request_data: dict = await request.json()
        request_id = request_data.get('request_id', None)

        worker_id, worker_url = select_worker(request.app, request.headers['algorithm_name'])

        task_predict_process_time = predict_processed_time(request_data)


        async with request.app['locks'][worker_id]:
            current_wait_time = request.app['wait_times'][worker_id]
            new_wait_time = current_wait_time + task_predict_process_time
            request.app['wait_times'][worker_id] = new_wait_time
            stdout_logger.info(f"Reqeust {request_id} to backend {worker_id}, add wait time {task_predict_process_time} s, task wait time {new_wait_time} s")


            # dynamic weight algorithm params update
            request.app['processing_tasks_sum'][worker_id] = request.app['processing_tasks_sum'][worker_id] + 1
            request.app['cumulative_times'][worker_id] = request.app['cumulative_times'][worker_id] + task_predict_process_time
            update_weights(request.app)


        # request to backend
        async with request.app['sessions'][worker_id].post(worker_url, json=request_data) as resp:
            if resp.status != 200:
                raise Exception(f"Worker node {worker_id}, return status {resp.status}")

            response_data: dict = await resp.json()
            status = resp.status
            task_start_time = response_data.get('start_process_time', receive)
            task_real_process_time = response_data.get('real_process_time')

            stdout_logger.info(f"Request {request_id} on worker node {worker_id}, real process time {task_real_process_time} s, predict wait time {new_wait_time} s")
        
        async with request.app['locks'][worker_id]:
            # dynamic weight algorithm params update
            request.app['processing_tasks_sum'][worker_id] = request.app['processing_tasks_sum'][worker_id] - 1

            # shortest pending time algorithem params update
            request.app['wait_times'][worker_id] = max(0, request.app['wait_times'][worker_id] - new_wait_time)
        

        response = {
            "status": status,
            "calculate_wait_time": new_wait_time,
            "real_wait_time": task_start_time - receive,
            "predict_process_time": task_predict_process_time, 
            "real_process_time": task_real_process_time,
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
