import time
from aiohttp import web
from pathlib import Path
import json
import xgboost as xgb
import traceback
from utils import setup_logger, load_xgboost_model          # type: ignore
import subprocess
import json
import asyncio
import redis.asyncio as aioredis
import logging
import os
from datetime import datetime

class RoundRobinState:
    def __init__(self):
        self.index = 0


class Worker:
    def __init__(self, worker_id):
        self.worker_id = worker_id

    def __repr__(self):
        return f"Worker({self.worker_id})"

class WorkerState:
    def __init__(self):
        self.waiting_time: float = 0.0
        self.lock = asyncio.Lock()


class Task:
    def __init__(self, request_data: dict):
        self.data = request_data
        task_id = self.data.pop('request_id')
        self.data['task_id'] = task_id
        self.data['manager_receive_time'] = time.time()
        self.data_str = json.dumps(self.data)

    def __str__(self):
        return f"{self.data.get('task_id')}:\n{dict({key: value for key, value in self.data.items() if key != 'task_id'})}"


class Result:
    def __init__(self, result_str: str):
        self.data: dict = json.loads(result_str)
        request_id = self.data.pop('task_id')
        self.data['request_id'] = request_id
        self.data_str = json.dumps(self.data)

def get_worker_nodes() -> list[Worker]:
    try:
        node_list_cmd = "docker node ls --filter 'role=worker' --format '{{.Hostname}}'"
        output = subprocess.check_output(node_list_cmd, shell=True, text=True).strip()
        workers = [Worker(line.strip()) for line in output.split("\n") if line.strip()]
        return workers
    except Exception as e:
        print(f"[{datetime.now().isoformat()}] {e}")
        return []

worker_nodes = get_worker_nodes()

###############################################################################
# global variable
###############################################################################

# setup XGBoost model
PARENT_DIR = Path(__file__).parent.parent
processing_time_model_path = PARENT_DIR / "models" / "xgboost_newest_model_4.json"
# processing_time_model_path = PARENT_DIR / "models" / "xgb_number_time.json"
xgboost_processing_time_model: xgb.Booster = load_xgboost_model(processing_time_model_path)

# logger settings
stdout_logger:logging.Logger = setup_logger('stdout_logger', PARENT_DIR / 'logs' / 'manager_stdout.log')
print(f"[{datetime.now().isoformat()}] logger started")


###############################################################################
# auxiliary function
###############################################################################

def predict_processing_time(request_data: dict) -> float:
    data = xgb.DMatrix([[request_data.get('number', 0)]])
    prediction = xgboost_processing_time_model.predict(data)
    return float(prediction[0])

async def return_to_zero(state: WorkerState):
    state.waiting_time = 0.0


async def select_worker_node(app: web.Application, predicted_processing_time: float, algo_name: str, task_id) -> Worker:
    global worker_nodes

    if not worker_nodes:
        raise RuntimeError("No worker nodes available")

    selected_worker = None
    selected_worker_id = None
    redis: aioredis.Redis = app['redis']
    if algo_name == 'round-robin':
        round_robin_state: RoundRobinState = app['rr_state']
        selected_worker = worker_nodes[round_robin_state.index]
        round_robin_state.index = (round_robin_state.index + 1) % len(worker_nodes)

        stdout_logger.info(f"[SelectWorker] Round-Robin selected: {selected_worker.worker_id}")

    elif algo_name == 'proposed':
        worker_state_map: dict[str, WorkerState] = app['worker_state_map']

        snapshot = []
        for worker in worker_nodes:
            queue_name = f'task_queue_{worker.worker_id}'
            queue_len = await redis.llen(queue_name)
            waiting_time = worker_state_map[worker.worker_id].waiting_time
            snapshot.append((worker, queue_len, waiting_time))
        
        for worker, queue_len, waiting_time in snapshot:
            if queue_len == 0 and waiting_time == 0.0:
                selected_worker = worker
                selected_worker_id = worker.worker_id
                break

        if not selected_worker:
            selected_worker, _, _ = min(snapshot, key=lambda x: x[2])
            selected_worker: Worker
            selected_worker_id = selected_worker.worker_id

        async with app['selected_worker_lock']:
            state = worker_state_map[selected_worker_id]
            app['predicted_waiting_time_map'][task_id] = state.waiting_time
            state.waiting_time += predicted_processing_time

        stdout_logger.info(f"[SelectWorker] Proposed selected: {selected_worker.worker_id}, updated worker state: {state.waiting_time:.2f}")

    elif algo_name == 'least-connections':
        # get redis length of task queues 
        job_counts = {}
        for worker in worker_nodes:
            queue_name = f'task_queue_{worker.worker_id}'
            job_counts[worker.worker_id] = await redis.llen(queue_name)

        selected_worker_id = min(job_counts, key=job_counts.get)
        selected_worker = next(worker for worker in worker_nodes if worker.worker_id == selected_worker_id)
        stdout_logger.info(f"[SelectWorker] Least-Connections selected: {selected_worker.worker_id}, job counts: {job_counts}")

    else:
        raise ValueError(f"Unknown algorithm: {algo_name}")

    return selected_worker


###############################################################################
# HTTP request handler：pushing task to request queue & fecth response 
###############################################################################


async def task_start_notice_listener(app: web.Application):
    redis: aioredis.Redis = app['redis']
    print(f'[{datetime.now().isoformat()}] [TaskStartListener] Start listening...')

    # Converge prediction time error over a long period of time

    # setting variable
    MAX_ALLOWED_ERROR = 1
        
    while True:
        try:
            res = await redis.blpop('task_start_notice_queue')
            if res:
                _, msg = res
                data = json.loads(msg)
                worker_id = data['worker_id']
                task_id = data['task_id']
                real_all_waiting_time_0 = data['real_all_waiting_time_0']

                if worker_id not in app['worker_state_map']:
                    print(f"[{datetime.now().isoformat()}] [TaskStartListener] Unknown worker_id: {worker_id}, skipping.")
                    continue
                

                # dynamicly correct predicted waiting time error
                state: WorkerState = app['worker_state_map'][worker_id]
                
                if task_id not in app['predicted_waiting_time_map']:
                    continue

                predicted_waiting_time = app['predicted_waiting_time_map'].get(task_id, 0)
                
                async with state.lock:       
                    error = predicted_waiting_time - real_all_waiting_time_0
                    state.waiting_time -= predicted_waiting_time
                    state.waiting_time = max(0, state.waiting_time)

                    if abs(error) > MAX_ALLOWED_ERROR:      # If the error is greater than 2 seconds, force assignment
                        corrected = (state.waiting_time + real_all_waiting_time_0) / 2
                        state.waiting_time = max(0.0, corrected)
                        app['predicted_waiting_time_correction'][task_id] = error
                        print(f"[{datetime.now().isoformat()}] [TaskStartListener] ⚠️ Hard correction: task {task_id} error={error:.2f}")
                    else:
                        if abs(error) > 1e-6:           # EWMA reverse
                            alpha = min(1.0, abs(error) / max(1.0, predicted_waiting_time))
                        else:
                            alpha = 0
                
                        new_waiting_time = alpha * real_all_waiting_time_0 + (1 - alpha) * state.waiting_time
                        state.waiting_time = max(0.0, new_waiting_time)
                        app['predicted_waiting_time_correction'][task_id] = error

                        print(f"[{datetime.now().isoformat()}] [TaskStartListener] ✅ EWMA: task {task_id}, α={alpha:.2f}, new={new_waiting_time:.2f}")
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] [TaskStartListener] Error: {e}")
            continue



async def result_listener(app: web.Application):
    redis: aioredis.Redis = app['redis']
    task_map: dict = app['task_map']
    worker_queue_names = [f'result_queue_{worker.worker_id}' for worker in worker_nodes]
    
    print(f"[{datetime.now().isoformat()}] [Listener] Listening queue: {worker_queue_names}")

    while True:
        try:
            res = await redis.blpop(worker_queue_names)
            if not res:
                continue
            _, msg = res
            result_obj: dict = json.loads(msg)
            task_id = result_obj.get('task_id')
            if task_id in task_map:
                future: asyncio.futures.Future = task_map.pop(task_id)
                future.set_result(msg)
                stdout_logger.info(f"[Listener] Get result, task_id: {task_id}")
            else:
                stdout_logger.warning(f"[Listener] No result task_id: {task_id}, message dropped.")
        except Exception as e:
            print(f"[{datetime.now().isoformat()}] {e}, {traceback.format_exc()}\n")
            stdout_logger.error(f"[Listener] Exception: {e}\n{traceback.format_exc()}")
            exit(1)


async def handle(request: web.Request):
    request_data = await request.json()
    task = Task(request_data)
    task_id = task.data.get('task_id')
    
    # construct future
    future = asyncio.get_event_loop().create_future()
    request.app['task_map'][task.data.get('task_id')] = future
    stdout_logger.info(f'Get new task: {task.data_str}')


    predicted_processing_time = predict_processing_time(task.data)        # to make string push to task queue
    algo_name = task.data.get('algo_name')
    selected_worker_node = await select_worker_node(request.app, predicted_processing_time, algo_name, task_id)

    # push task
    redis: aioredis.Redis = request.app['redis']

    task_queue_name = f'task_queue_{selected_worker_node.worker_id}'
    await redis.rpush(task_queue_name, task.data_str)

    # get result
    try: 
        result = await asyncio.wait_for(future, timeout=None)
        result_data= Result(result).data
        result_data['selected_worker_id'] = selected_worker_node.worker_id
        result_data['predicted_processing_time'] = predicted_processing_time
        
        result_request_id = result_data['request_id']

        if algo_name == 'proposed':
            predicted_waiting_time_map: dict = request.app['predicted_waiting_time_map']
            result_data['predicted_waiting_time'] = predicted_waiting_time_map.pop(result_request_id, 0)

            predicted_waiting_time_map_correct: dict = request.app['predicted_waiting_time_correction']
            result_data['predicted_waiting_time_correction'] = predicted_waiting_time_map_correct.pop(result_request_id, 0)

        return web.json_response(result_data)
    except Exception as e:
        request.app['task_map'].pop(task_id, None)
        return web.json_response({'status': 'timeout', 'task_id': task_id, 'error': str(e), 'details': traceback.format_exc()})


###############################################################################
# app initialization
###############################################################################
async def init_app(app: web.Application):
    global worker_nodes
    app['redis'] = aioredis.from_url('redis://192.168.0.100:6379', decode_responses=True)

    # clear redis unfinish task
    redis: aioredis.Redis = app['redis']
    for worker in worker_nodes:
        await redis.delete(f'task_queue_{worker.worker_id}')
        await redis.delete(f'result_queue_{worker.worker_id}')
    await redis.delete('task_start_notice_queue')

    app['task_map'] = {}
    app['listener'] = asyncio.create_task(result_listener(app))

    """ algorithm parameters """
    # round-robin
    app['rr_state'] = RoundRobinState()

    app['task_start_listener'] = asyncio.create_task(task_start_notice_listener(app))

    # proposed
    app['worker_state_map'] = { worker.worker_id: WorkerState() for worker in worker_nodes }
    app['predicted_waiting_time_map'] = {}
    app['predicted_waiting_time_correction'] = {}

    # furture predicting start timestamp

    # lock
    app['selected_worker_lock'] = asyncio.Lock()

    # sliced window
    # app['waiting_time_window'] = []

    pass

async def cleanup(app):
    redis_client: aioredis.Redis  = app['redis']
    await redis_client.aclose()
    print(f"[{datetime.now().isoformat()}] Redis connection closed.")

    states: dict[str, WorkerState] = app['worker_state_map']
    # release lock
    try:
        for state in states.values():
            if state.lock.locked():
                state.lock.release()
    except RuntimeError:
        pass

    tasks = {t for t in asyncio.all_tasks() if t is not asyncio.current_task()}

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)
    print(f"[{datetime.now().isoformat()}] All background tasks cancelled")

    # kill -9
    os._exit(0)

def create_app() -> web.Application:
    app = web.Application()
    app.router.add_post("/", handle)
    app.on_startup.append(init_app)
    app.on_cleanup.append(cleanup)
    print(f"[{datetime.now().isoformat()}] Create app...")
    return app


if __name__ == "__main__":
    try:
        web.run_app(create_app(), host='192.168.0.100', port=8199)
    except KeyboardInterrupt:
        print(f"\n[{datetime.now().isoformat()}] Exit")
        os._exit(0)
