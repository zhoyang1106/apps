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
import xgboost as xgb
import paramiko
import random
from sklearn.linear_model import LinearRegression
import joblib
from concurrent.futures import ProcessPoolExecutor


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


SHORTEST_PENDING_TIME_WORKER = None

PARENT_DIR = Path(__file__).parent

# xgboost model
process_model_path = (PARENT_DIR / "models" / "xgb_number_time_linear.json")
xgboost_proc_model = xgb.XGBRegressor()
xgboost_proc_model.load_model(process_model_path)

# model_file = Path.cwd() / 'manage-server' / 'models' / 'number_time_LinearR.joblib'
# linear_model: LinearRegression = joblib.load(model_file)

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
        self.pred_processed_time = self.predict_processed_time()
        self.serving_worker_number = None
        self.wait_time = float(0)
        # record how long time until receive response
        self.until_response_time = 0

        # resource usage
        self.hdd_usage = 0
        self.mem_usage = 0

    # xgboost
    def predict_processed_time(self):
        # data = xgb.DMatrix([[self.request_data.get('number')]])
        data = [[self.request_data.get('number')]]
        prediction = xgboost_proc_model.predict(data)
        return float(prediction[0])


    # linear
    # def predict_processed_time(self):
        
    #     data = [[self.request_data.get('number')]]
    #     prediction = linear_model.predict(data)

    #     # # linear
    #     # data = self.request_data.get('number')
    #     # predict = linear_model.coef_[0] * data + linear_model.intercept_
    #     # # return prediction[0]

    #     return predict

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

        # tasks queue on manager ( to calculate not to control )
        self.tasks_queue = Queue()

        # resource usage
        self.hdd_usage = 0
        self.mem_usage = 0

        self.max_hdd_usage = 0
        self.max_mem_usage = 0

        # session for worker connector
        self.session = None


        self.ssh_client = paramiko.SSHClient()
        self.ssh_hostname = self.ip
        self.ssh_password = 'raspberrypi'
        self.ssh_username = 'pi'
        self.ssh_port = 22

        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh_client.connect(self.ssh_hostname, port=self.ssh_port, username=self.ssh_username, password=self.ssh_password)


    async def start_session(self):
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0), timeout=aiohttp.ClientTimeout(total=None))

    async def close_session(self):
        await self.session.close()

    # update timer with 
    async def update_wait_time(self):
        while True:
            await asyncio.sleep(self.update_interval)
            self.wait_time -= self.update_interval
            if self.wait_time < 0:
                self.wait_time = 0


    async def hdd_usage_handle(self):
        cmd = """df -B1"""
        try:
            while True:
                stdin, stdout, stderr = self.ssh_client.exec_command(command=cmd)
                output = stdout.read().decode()
                # print("Output of [ df -B1 ]: ", output)

                total_blocks = 0
                for line in output.splitlines():
                    parts = line.split()
                    if len(parts) > 0 and parts[0].startswith('/dev'):
                        blocks = int(parts[1])
                        total_blocks += blocks
                        
                    
                self.hdd_usage = total_blocks
                await asyncio.sleep(0.3)
        
        # Stop with error or interrupt
        except asyncio.CancelledError:
            print("Task was cancelled.")
            raise

        except Exception as e:
            error_message = traceback.format_exc()
            print(error_message)
            exit(1)
        finally:
            self.ssh_client.close()


    async def mem_usage_handle(self):
        cmd = """cat /proc/meminfo"""
        try:
            while True:
                stdin, stdout, stderr = self.ssh_client.exec_command(command=cmd)
                output = stdout.read().decode()
                
                # 解析 /proc/meminfo 输出，提取内存使用情况
                mem_info = {}
                for line in output.splitlines():
                    parts = line.split(':')
                    if len(parts) == 2:
                        key = parts[0].strip()
                        value = parts[1].strip().split()[0]  # 提取值，单位为 KB
                        mem_info[key] = int(value) * 1024  # 将 KB 转为字节

                mem_total = mem_info.get('MemTotal', 0)
                mem_free = mem_info.get('MemFree', 0)
                mem_available = mem_info.get('MemAvailable', 0)
                mem_used = mem_total - mem_free

                # print(f"Total Memory: {mem_total} bytes")
                # print(f"Free Memory: {mem_free} bytes")
                # print(f"Available Memory: {mem_available} bytes")
                # print(f"Used Memory: {mem_used} bytes")

                # 等待 1 秒钟再监测
                self.mem_usage = mem_used
                await asyncio.sleep(0.3)

        except asyncio.CancelledError:
            print("Memory monitoring task was cancelled.")
            raise
        except Exception as e:
            error_message = traceback.format_exc()
            print(f"Error occurred: {error_message}")
        finally:
            self.ssh_client.close()
            print("SSH connection closed.")

UPDATE_INTERVAL = 0.02

WORKERS = [
    Worker(ip='192.168.0.150', port=8080, update_interval=UPDATE_INTERVAL),
    Worker(ip='192.168.0.151', port=8080, update_interval=UPDATE_INTERVAL),
    Worker(ip='192.168.0.152', port=8080, update_interval=UPDATE_INTERVAL),
]


ROUND_ROUBIN_WORKER_INDEX = 0

async def start_sessions():
    global WORKERS
    for worker in WORKERS:
        await worker.start_session()


def update_shortest_pending_time_worker():
    global SHORTEST_PENDING_TIME_WORKER
    while True:
        SHORTEST_PENDING_TIME_WORKER = min(enumerate(worker.wait_time for worker in WORKERS), key=lambda x: x[1])[0]
        # time.sleep(0.1)


# multi IP addresses
def choose_url_algorithm(name=None, **kwargs):
    global WORKERS, ROUND_ROUBIN_WORKER_INDEX, SHORTEST_PENDING_TIME_WORKER
    chosen_worker = None
    new_task: Task = kwargs.get('new_task')
    # use params from kwargs

    if not name or name == 'round-robin': # round robin
        r_start = time.time()
        worker_index = ROUND_ROUBIN_WORKER_INDEX
        ROUND_ROUBIN_WORKER_INDEX = (ROUND_ROUBIN_WORKER_INDEX + 1) % len(WORKERS)
        chosen_worker = WORKERS[worker_index]
        logging.info(f"round-robin ran time: {time.time() - r_start} s")
    
    elif name == 'proposed':
        s_start = time.time()
        # loop proposed function
        chosen_worker = SHORTEST_PENDING_TIME_WORKER

        logging.info(f"proposed ran time: {time.time() - s_start} s")
        

    logging.info("-- " * 10)
    logging.info(f"new task process time: {new_task.pred_processed_time} s")
    for worker in WORKERS:
        logging.info(f"[ {worker.wait_time} ]") 
    
    random_worker_index = int(random.randint(0, 2))
    return chosen_worker if chosen_worker else WORKERS[random_worker_index]


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
        
        new_task.wait_time = chosen_worker.wait_time
        chosen_worker.wait_time += new_task.pred_processed_time     

        async with chosen_worker.lock:
            chosen_worker.current_task = new_task
            chosen_worker.processing_cnt += 1

        # put into worker queue
        await chosen_worker.tasks_queue.put(new_task)

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
        
        logging.info(f'{"-" * 40}\n')
        logging.info(f"Before {time.time()} Request number {new_task.request_data.get('number')}")
        logging.info(f"task prediction process time {new_task.pred_processed_time}")
        logging.info(f"worker node nummber:{new_task.serving_worker_number}")
        logging.info(f"processing_cnt: {chosen_worker.processing_cnt}")
        logging.info(f"task wait time: {new_task.wait_time}")
        logging.info(f"total predict response time: {new_task.wait_time + new_task.pred_processed_time}")
        total_response_time_prediction = new_task.wait_time + new_task.pred_processed_time
        before_forward_timestamp = time.time()
        before_forward_time = before_forward_timestamp - manager_received_timestamp

        await chosen_worker.tasks_queue.get()

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
            data['total_response_time_prediction'] = total_response_time_prediction
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
    global WORKERS
    for worker in WORKERS:
        asyncio.create_task(worker.hdd_usage_handle())
        asyncio.create_task(worker.mem_usage_handle())
        asyncio.create_task(worker.update_wait_time())

    loop = asyncio.get_running_loop()
    exec =  ProcessPoolExecutor()
    
    loop.run_in_executor(exec, update_shortest_pending_time_worker)

    print("Start min pending time worker update")
    print("SSH connect started")
    print("Workers' timer has started")

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
    # print(Path(__file__).parent)

