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
    
    # shortest algorithm
    if algorithm_name == 'shortest':
        min_finish = float('inf')
        now = time.time()
        for worker_id, ft in app['finish_times'].items():
            waiting = max(0, ft - now)
            if waiting < min_finish:
                min_finish = waiting
                selected_worker = worker_id
        return selected_worker, WORKERS[selected_worker]
    
    # round-robin algorithm
    elif algorithm_name == 'round-robin':
        selected_worker = ROUND_ROBIN_INDEX
        ROUND_ROBIN_INDEX = (ROUND_ROBIN_INDEX + 1) % len(WORKERS)
        return selected_worker, WORKERS[selected_worker]
    
    # min-entropy algorithm
    elif algorithm_name == "min-entropy":
        selected_worker = max(app['dynamic_weight'], key=app['dynamic_weight'].get)
        return selected_worker, WORKERS[selected_worker]
    
    # least-connections algorithm
    elif algorithm_name == "least-connect":
        selected_worker = min(app['least_connect'], key=app['least_connect'].get)
        return selected_worker, WORKERS[selected_worker]
    
    else:
        return 0, WORKERS[0]


# timer task
async def countdown_task(app, interval, worker_id):
    """Indecrease finish_times timestamp value"""
    try:
        while True:
            # async with app['locks'][worker_id]:  
            new_wait_time = app['wait_times'][worker_id] - interval
            if new_wait_time < 0:
                new_wait_time = 0
            app['wait_times'][worker_id] = new_wait_time
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        print("Countdown task was canceled")



# start tasks
async def on_startup(app):
    # app['wait_times'] = {i: 0 for i in range(len(WORKERS))}
    app['finish_times'] = {i: 0 for i in range(len(WORKERS))} 
    app['locks'] = {i: asyncio.Lock() for i in range(len(WORKERS))}
    app['sessions'] = {
        i: ClientSession(timeout=ClientTimeout(None)) for i in range(len(WORKERS))
    }
    # app['countdown_tasks'] = {i: asyncio.create_task(countdown_task(app, interval=0.1, worker_id=i)) for i in range(len(WORKERS))}
    app['correct_finish_time_periodically'] = asyncio.create_task(correct_finish_time_periodically(app, interval=1))
    app['processing_tasks_sum'] = {i: 0 for i in range(len(WORKERS))}
    app['least_connect'] = {i: 0 for i in range(len(WORKERS))}
    app['cumulative_times'] = {i: 0 for i in range(len(WORKERS))}
    app['dynamic_weight'] = {i: 1 / len(WORKERS) for i in range(len(WORKERS))}
    print("All countdown tasks and client sessions started.")


def update_weights(app, process_time, worker_id):
    alpha = 1.0     # complex weight
    beta = 0.2      # quene size weight

    task_counts = list(app['processing_tasks_sum'].values())
    new_weight = max(0, 1 / (1 + alpha * process_time + task_counts[worker_id] * beta) - app['dynamic_weight'][worker_id])

    weights = []

    app['dynamic_weight'][worker_id] = new_weight
    for n, t in zip(task_counts, app['dynamic_weight'].values()):
        weights.append(1 / (1 + alpha * t + n * beta))

    total = sum(weights)
    for index, weight in enumerate(weights):
        app['dynamic_weight'][index] = weight / total


async def correct_finish_time_periodically(app, interval):
    while True:
        for worker_id in range(len(WORKERS)):
            async with app['locks'][worker_id]:
                ft = app['finish_times'][worker_id]
                if ft < time.time():
                    app['finish_times'][worker_id] = time.time()
        await asyncio.sleep(interval)


async def handle(request: web.Request):
    try:
        request_data: dict = await request.json()
        request_id = request_data.get('request_id', None)

        algorithm_name = request.headers['algorithm_name']

        worker_id, worker_url = select_worker(request.app, algorithm_name)

        task_predict_process_time = predict_processed_time(request_data)

        now = time.time()

        # least-connections algorithm
        if algorithm_name == 'least-connect':
            async with request.app['locks'][worker_id]:
                request.app['least_connect'][worker_id] += 1

        # shortest-pending-time algorithm
        elif algorithm_name == 'shortest':
            async with request.app['locks'][worker_id]:
                ft = request.app['finish_times'][worker_id]
                pending_time_estimated = max(0, ft - now)

                new_finish_time = max(ft, now) + task_predict_process_time
                request.app['finish_times'][worker_id] = new_finish_time
            
            stdout_logger.info(f"[SHORT] Request {request_id} -> worker {worker_id}, pending~{pending_time_estimated:.2f}s, predicted {task_predict_process_time:.2f}s")

        # min-enrtopy algorithm params update
        elif algorithm_name == 'min-entropy':
            async with request.app['locks'][worker_id]:
                request.app['processing_tasks_sum'][worker_id] = request.app['processing_tasks_sum'][worker_id] + 1
                update_weights(request.app, worker_id=worker_id, process_time=task_predict_process_time)
                stdout_logger.info(f"[REQUEST] {request_id} Weight updated.")
                stdout_logger.info(f"[WEIGHT LIST]: [{[w for w in request.app['dynamic_weight'].values()]}]")

        # request to backend
        async with request.app['sessions'][worker_id].post(worker_url, json=request_data) as resp:
            if resp.status != 200:
                raise Exception(f"Worker node {worker_id}, return status {resp.status}")

            response_data: dict = await resp.json()

        # shortest-pending-time
        if algorithm_name == 'shortest':
            task_real_process_time = response_data.get('real_process_time')
            real_wait_time = max(0, response_data.get('start_process_time', now) - now)


            difference = pending_time_estimated -real_wait_time
            async with request.app['locks'][worker_id]:
                current_ft = request.app['finish_times'][worker_id]
                if current_ft > time.time():
                    corrected_ft = current_ft - difference

                    if corrected_ft < time.time():
                        corrected_ft = time.time()

                    request.app['finish_times'][worker_id] = corrected_ft

                    stdout_logger.info(f"[CORRECT] Worker {worker_id} finish_time from {current_ft:.2f} -> {corrected_ft:.2f}, (difference={difference:.2f})")

            stdout_logger.info(f"Request {request_id} on worker node {worker_id}, real process time {task_real_process_time} s, predict wait time {pending_time_estimated} s")

            response_data = response_data | {
                "worker": worker_id,
                "status": resp.status,
                "calculate_wait_time": pending_time_estimated,
                "real_wait_time": real_wait_time,
                "predict_process_time": task_predict_process_time,
                "real_process_time": task_real_process_time,
            }

            print(response_data)

        # min-entropy
        elif algorithm_name == 'min-entropy':
            async with request.app['locks'][worker_id]:
                request.app['processing_tasks_sum'][worker_id] = request.app['processing_tasks_sum'][worker_id] - 1

        
        # least-connect
        elif algorithm_name == 'least-connect':
            async with request.app['locks'][worker_id]:
                request.app['least_connect'][worker_id] -= 1

        return web.json_response(response_data)
    
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
