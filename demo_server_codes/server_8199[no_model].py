import aiohttp
from aiohttp import web
import time
from datetime import datetime
import traceback
import asyncio
from asyncio import Queue
import logging
from pathlib import Path
from xgboost import Booster
import xgboost
import paramiko
import random

##
# No Model


TIME_SLOT_LENGTH = 1  # ms
SIMULATION_ITER_MAX = 1000 # max number of time slot
TASK_GENERATE_RATE = 30 # one task for three time slots
MEM_CAPACITY = 500*1_000_000 # 500 MB (use any number)
HDD_CAPACITY = 1.5*1_000_000_000 # 1.5GB (use any number)
num_tasks_generated = 0
MAX_NUM_TASKS_TO_GENERATE = 3

# initialize
NUM_WORKERS = 3
task_queues_in_processing = [[] for w in range(NUM_WORKERS)] # tasks that are being processed
tasks_done_processing = []  # tasks that are done processing


PARENT_DIR = Path(__file__).parent.parent

# LOG file
log_path = PARENT_DIR / 'logs' / f"{Path(__file__).stem}.log"
print("Log Path:", log_path)
logging.basicConfig(filename=log_path, level=logging.INFO, filemode='w')





# tasks waiting queue
# for all tasks waiting timer
class Task:
    def __init__(self, **kwargs):
        # request data
        self.request_data: dict = kwargs.get('request_data')
        self.headers: dict = kwargs.get('headers')
        self.worker: Worker = None
        self.target_url = None
        self.serving_worker_number = None
        # record how long time until receive response
        self.until_response_time = 0

class Worker:
    def __init__(self, **kwargs):
        self.ip = kwargs.get('ip')
        self.port = kwargs.get('port')
        self.url = f'http://{self.ip}:{self.port}'
        self.lock = asyncio.Lock()
        
        self.update_interval = kwargs.get('update_interval')
        self.wait_time = float(0)

        self.current_task = None

        # every status tasks sum on worker
        self.processing_cnt = 0
        self.received_cnt = 0
        self.finished_cnt = 0

        # resource usage
        self.hdd_usage = 0
        self.mem_usage = 0

        self.max_hdd_usage = 0
        self.max_mem_usage = 0

        # session for worker connector
        self.session = None


    async def start_session(self):
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0), timeout=aiohttp.ClientTimeout(total=None))

    async def close_session(self):
        await self.session.close()

WORKERS = [
    Worker(ip='192.168.0.150', port=8080),
    Worker(ip='192.168.0.151', port=8080),
    Worker(ip='192.168.0.152', port=8080),
]

ROUND_ROUBIN_WORKER_INDEX = 0

async def start_sessions():
    global WORKERS
    for worker in WORKERS:
        await worker.start_session()


# multi IP addresses
def choose_url_algorithm(name=None, **kwargs):
    global WORKERS, ROUND_ROUBIN_WORKER_INDEX
    # use params from kwargs
    new_task: Task = kwargs.get('new_task')
    response_time = [ (worker.wait_time + new_task.pred_processed_time, worker) for worker in WORKERS ]

    if not name or name == 'round-robin': # round robin
        worker_index = ROUND_ROUBIN_WORKER_INDEX
        ROUND_ROUBIN_WORKER_INDEX = (ROUND_ROUBIN_WORKER_INDEX + 1) % len(WORKERS)
        return WORKERS[worker_index]
    
    elif name == 'shortest':
        shortest_time = float("inf")
        chosen_worker = None
        for item in response_time:
            if item[0] < shortest_time:
                shortest_time = item[0]
                chosen_worker = item[1]

        if chosen_worker:
            return chosen_worker
        else:
            return WORKERS[0]
    else:
        return WORKERS[0]

async def handle_new_task(request_data: dict, headers: dict):
    global WORKERS
    new_task = Task(request_data=request_data, headers=headers)
    
    # set new_task params
    # ...
    # ...
    # predict process time
    
    try:
        chosen_worker = None
        
        for worker in WORKERS:
            if worker.processing_cnt == 0:
                chosen_worker = worker
                break
        
        if not chosen_worker:
            algorithm_name = new_task.headers.get('algo_name')
            logging.info(algorithm_name)
            chosen_worker = choose_url_algorithm(name=algorithm_name, new_task=new_task)
        
        if not chosen_worker:
            raise Exception("chosen worker is None")

        new_task.worker = chosen_worker
        new_task.target_url = chosen_worker.url
        new_task.serving_worker_number = WORKERS.index(chosen_worker)
        
        chosen_worker.current_task = new_task

        async with chosen_worker.lock:
            chosen_worker.processing_cnt += 1

        # put into worker queue
        return chosen_worker, new_task
    except Exception as e:
        error_message = traceback.format_exc()
        print(error_message)
        exit(1)
    

# handle request main function
async def request_handler(request: web.Request):
    try:
        # received time
        manager_received_timestamp = time.time()
        logging.info(f"received time: {manager_received_timestamp}\n")

        # generate task and put into manager tasks queue
        request_data = await request.json()
        chosen_worker, new_task = await handle_new_task(request_data, request.headers)
        
        processing_cnt = chosen_worker.processing_cnt
        # fetch queue first task and send
        
        print('-' * 40, end='\n')
        print("Before", time.time(), f"Request number {new_task.request_data.get('number')}")
        print(f"worker node nummber:{new_task.serving_worker_number}")
        print("processing_cnt:", chosen_worker.processing_cnt)
        before_forward_timestamp = time.time()
        before_forward_time = before_forward_timestamp - manager_received_timestamp

        # send this task to worker node
        async with chosen_worker.session.post(url=chosen_worker.url, json=new_task.request_data, headers=new_task.headers) as response:
            data: dict = await response.json()

            # 记录任务结束时间

            async with chosen_worker.lock:
                chosen_worker.finished_cnt += 1
                chosen_worker.processing_cnt -= 1
            if "error" in data.keys():
                data["success"] = 0
            else:
                data["success"] = 1

            # update response data
            data["chosen_ip"] = chosen_worker.ip
            data['processed_time'] = data.pop("real_process_time")
            data['jobs_on_worker_node'] = processing_cnt
            data['real_task_wait_time'] =  data['start_process_time'] - manager_received_timestamp
            data['before_forward_time'] = before_forward_time
            data['pred_task_wait_time'] = new_task.wait_time
            data['before_forward_timestamp'] = before_forward_timestamp
            
            logging.info(f'{"-" * 40}\n')
            logging.info(f'{data}\n')
            logging.info(f"{'Before waiting jobs:':<50}{processing_cnt:<20}\n")
            logging.info(f"{'worker wait time:':<50}{data['real_task_wait_time']:<20}\n")
            logging.info(f"{'Datetime:':<50}{datetime.ctime(datetime.now()) :<20}\n")

            logging.info(f"{'-' * 40}\n")
            logging.info(f"After {time.time()} Request number {new_task.request_data.get('number')}")
            logging.info(f"worker node nummber:{new_task.serving_worker_number}")
            logging.info(f"processing_cnt: {chosen_worker.processing_cnt} ")
            
            return web.json_response(data)

    except Exception:
        error_message = traceback.format_exc()
        print(error_message)
        return web.json_response({"error": error_message, "data": data}, status=500)


async def on_shutdown(app):
    global WORKERS
    for worker in WORKERS:
        await worker.close_session()

async def server_app_init():
    app = web.Application()
    app.router.add_post("", request_handler)
    app.on_startup.append(lambda app: start_sessions())
    app.on_cleanup.append(on_shutdown)

    return app
def server_run():
    try:
        app = server_app_init()
        web.run_app(app, host='0.0.0.0', port=8199)
    except Exception as e:
        print(f"[ {datetime.ctime(datetime.now())} ]")
        error_message = traceback.format_exc()
        print(error_message)


if __name__ == "__main__":
    server_run()
