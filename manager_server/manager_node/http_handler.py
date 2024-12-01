# - manager_node/http_handler.py -

import typing
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
from concurrent.futures import ProcessPoolExecutor
import json

TIME_SLOT_LENGTH = 1  # ms
SIMULATION_ITER_MAX = 1000 # max number of time slot
TASK_GENERATE_RATE = 30 # one task for three time slots
MEM_CAPACITY = 500*1_000_000 # 500 MB (use any number)
HDD_CAPACITY = 1.5*1_000_000_000 # 1.5GB (use any number)
NUM_TASKS_GENERATED = 0
MAX_NUM_TASKS_TO_GENERATE = 3

# initialize
NUM_WORKERS = 3
task_queues_in_processing = [[] for w in range(NUM_WORKERS)] # tasks that are being processed
tasks_done_processing = []  # tasks that are done processing


SHORTEST_PENDING_TIME_WORKER = None
LEATEST_CONNECTOR_WORKER = None

PARENT_DIR = Path(__file__).parent.parent

# model_file = Path.cwd() / 'manage-server' / 'models' / 'number_time_LinearR.joblib'
# linear_model: LinearRegression = joblib.load(model_file)



### log file config

# stdout logger
stdout_logger = logging.getLogger(name='stdout_logger')
stdout_logger.setLevel(logging.INFO)


# chronograph logger
chronograph_logger = logging.getLogger(name='chronograph_logger')
chronograph_logger.setLevel(logging.INFO)


# stdout log
stdout_log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stdout_log_path = PARENT_DIR / 'logs' / f"{Path(__file__).stem}.log"
stdout_log_path.parent.mkdir(parents=True, exist_ok=True)
stdout_file_handler = logging.FileHandler(stdout_log_path, mode='w')
stdout_file_handler.setLevel(logging.INFO)
stdout_file_handler.setFormatter(stdout_log_formatter)
stdout_logger.addHandler(stdout_file_handler)


## chronograph log file
chronograph_log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
chronograph_log_path = PARENT_DIR / 'logs' / 'chronograph.log'
chronograph_log_path.parent.mkdir(parents=True, exist_ok=True)
chronograph_file_handler = logging.FileHandler(chronograph_log_path, mode='w')
chronograph_file_handler.setLevel(logging.INFO)
chronograph_file_handler.setFormatter(chronograph_log_formatter)
chronograph_logger.addHandler(chronograph_file_handler)


executor = ProcessPoolExecutor()


# tasks waiting queue
# for all tasks waiting timer

class Task:
    pass
class ManageNode:
    pass
class Worker:
    pass


class Task:
    def __init__(self, **kwargs):
        # request data
        self.request_data: dict = kwargs.get('request_data')
        self.headers: dict = kwargs.get('headers')
        self.pred_processed_time = 0
        self.worker_id = None
        self.wait_time = 0

        # resource usage
        self.hdd_usage = 0
        self.mem_usage = 0



class Worker:
    def __init__(self, **kwargs):
        self.ip = kwargs.get('ip')
        self.port = kwargs.get('port')
        self.url = f'http://{self.ip}:{self.port}'
        self.lock = asyncio.Lock()
        self.id = kwargs.get("id", f'x{random.randint(0, 1000)}')
        # self.timer_lock = multiprocessing.Lock()
        
        self.wait_time = 0.0
        self.wait_time_update_interval = kwargs.get('update_interval')
        self.wait_time_update_process = None

        # every status tasks sum on worker
        self.processing_cnt = 0
        self.received_cnt = 0
        self.finished_cnt = 0

        self.stop_event = asyncio.Event()
        self.hdd_task = None
        self.mem_task = None

        # resource usage
        self.hdd_usage = 0
        self.mem_usage = 0

        self.max_hdd_usage = 0
        self.max_mem_usage = 0

        # session for worker connector
        self.session = None
        
        # timer process executor
        self.ssh_executor = executor

        self.ssh_client = paramiko.SSHClient()
        self.ssh_hostname = self.ip
        self.ssh_password = 'raspberrypi'
        self.ssh_username = 'pi'
        self.ssh_port = 22

        self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh_client.connect(self.ssh_hostname, port=self.ssh_port, username=self.ssh_username, password=self.ssh_password)


        # worker scores
        """ (connect sum + request number sum) / worker load max limit (/cpu) [0.8] """
        self.worker_load_score = 0
        self.sum_request_number = asyncio.Queue()
        self.sum_request_number_avg = 0
        self.worker_load_limit = kwargs.get('cpu_limit')


    async def update_score(self, new_task: Task): # type: ignore
        self.sum_request_number.put_nowait(new_task.request_data['number'])
        self.sum_request_number_avg = self.sum_request_number_avg  / ((self.sum_request_number_avg + self.sum_request_number.get_nowait()) / 2)
        self.worker_load_score = (((self.processing_cnt - 1) / self.processing_cnt)  + self.sum_request_number_avg) / self.worker_load_limit


    async def start_session(self):
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0), timeout=aiohttp.ClientTimeout(total=None))

    async def close_session(self):
        await self.session.close()
        self.ssh_client.close()


    def wait_time_update(self):
        while True:
            with self.lock:
                self.wait_time -= self.wait_time_update_interval



    def run_ssh_command(self, cmd: str):
        stdin, stdout, stderr = self.ssh_client.exec_command(command=cmd)
        output = stdout.read().decode()
        return output

    def hdd_output_parse(self, output: str):
        total_blocks = 0
        for line in output.splitlines():
            parts = line.split()
            if len(parts) > 0 and parts[0].startswith('/dev'):
                blocks = int(parts[1])
                total_blocks += blocks

        return total_blocks

    def mem_output_parse(self, output: str):
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
        return mem_used

    def hdd_usage_handle(self):
        cmd = """df -B"""
        try:
            while not self.stop_event.is_set():
                output = self.run_ssh_command(cmd=cmd)
                # 处理逻辑
                total_blocks = self.hdd_output_parse(output)
                self.hdd_usage = total_blocks

        except Exception as e:
            # 捕获其他异常
            print(e)
            error_message = traceback.format_exc()
            print(f"Error in HDD monitoring task: {error_message}")
            exit(1)
            

    def mem_usage_handle(self):
        cmd = """cat /proc/meminfo"""
        try:
            while not self.stop_event.is_set():
                output = self.run_ssh_command(cmd=cmd)
                # 处理逻辑
                self.mem_usage = self.mem_output_parse(output)

        except Exception as e:
            print(e)
            # 捕获其他异常
            error_message = traceback.format_exc()
            print(f"Error in Memory monitoring task: {error_message}")
            exit(1)


class ManagerNode:
    def __init__(self, **kwargs):
        self.round_robin_index = 0
        self.xgboost_model = kwargs.get("xgboost_model", None)
        self.workers: typing.List[Worker] = kwargs.get('workers', [])
        self.process_executor = executor

        print("SSH connect started")

    async def start_worker_hdd_mem_task(self):
        loop = asyncio.get_running_loop()
        for worker in self.workers:
            # worker.hdd_task = asyncio.create_task(worker.hdd_usage_handle())
            # worker.mem_task = asyncio.create_task(worker.mem_usage_handle())
            loop.run_in_executor(self.process_executor, worker.hdd_usage_handle)
            loop.run_in_executor(self.process_executor, worker.mem_usage_handle)

            
    
    async def start_worker_update_time_process(self):
        loop = asyncio.get_running_loop()
        for worker in self.workers:
            # worker.wait_time_update_process = asyncio.create_task(worker.wait_time_update())
            loop.run_in_executor(None, worker.worker.wait_time_update)


    def predict_processed_time(self, task: Task):
        data = xgb.DMatrix([[task.request_data.get('number')]])
        # data = [[self.request_data.get('number')]]
        prediction = self.xgboost_model.predict(data)
        return float(prediction[0])

    
    async def choose_worker(self, **kwargs):
        
        chosen_worker = None

        # default round-robin
        if len(self.workers):
            algorithm_name = kwargs.get('algorithm_name', 'round-robin')

        # use params from kwargs
        if not algorithm_name or algorithm_name == 'round-robin': # round robin
            chosen_worker = self.workers[self.round_robin_index]
            self.round_robin_index = (self.round_robin_index + 1) % len(self.workers)
        
        elif algorithm_name == 'shortest':
            chosen_worker = min(self.workers, key=lambda w: w.wait_time)

        
        elif algorithm_name == 'least':
            chosen_worker = min(self.workers, key=lambda w: w.processing_cnt)

        elif algorithm_name == 'lowest-score':
            chosen_worker = min(self.workers, key=lambda w: w.worker_load_score)
            chosen_worker.update_score(new_task=kwargs.get('new_task'))
            
        else:
            random_worker_index = random.randint(0, len(self.workers))

        return chosen_worker if chosen_worker else self.workers[random_worker_index]

    async def handle_new_task(self, request_data: dict, headers: dict):
        new_task = Task(request_data=request_data, headers=headers)
        new_task.pred_processed_time = self.predict_processed_time(new_task)
        # set new_task params
        # ...
        # ...
        # predict process time
        
        try:
            chosen_worker = None
            
            for worker in self.workers:
                if worker.processing_cnt == 0:
                    chosen_worker = worker
                    chosen_worker.wait_time = 0
                    break
            
            if not chosen_worker:
                algorithm_name = new_task.headers.get('algo_name')
                chosen_worker = await self.choose_worker(name=algorithm_name, new_task=new_task)

            if not chosen_worker:
                raise Exception("chosen worker is None")

            new_task.worker_id = chosen_worker.id
            new_task.wait_time = chosen_worker.wait_time

            async with chosen_worker.lock:
                chosen_worker.wait_time += new_task.pred_processed_time     
            chosen_worker.processing_cnt += 1
                
            return chosen_worker, new_task
        except Exception as e:
            error_message = traceback.format_exc()
            print(error_message)
            stdout_logger.error(error_message)
            exit(1)

    async def start_sessions(self):
        for worker in self.workers:
            await worker.start_session()

    # handle request main function
    async def request_handler(self, request: web.Request):
        try:
            # received time
            manager_received_timestamp = time.time()
            stdout_logger.info(f"received time: {manager_received_timestamp}\n")

            # generate task and put into manager tasks queue
            request_data = await request.json()
            chosen_worker, new_task = await self.handle_new_task(request_data, request.headers)
            
            processing_cnt = chosen_worker.processing_cnt
            # fetch queue first task and send
            
            stdout_logger.info(f'{"-" * 40}\n')
            stdout_logger.info(f"Before {time.time()} Request number {new_task.request_data.get('number')}")
            stdout_logger.info(f"task prediction process time {new_task.pred_processed_time}")
            stdout_logger.info(f"processing_cnt: {chosen_worker.processing_cnt}")
            stdout_logger.info(f"task wait time: {new_task.wait_time}")
            stdout_logger.info(f"total predict response time: {new_task.wait_time + new_task.pred_processed_time}")


            total_response_time_prediction = new_task.wait_time + new_task.pred_processed_time
            before_forward_timestamp = time.time()
            before_forward_time = before_forward_timestamp - manager_received_timestamp


            # send this task to worker node
            async with chosen_worker.session.post(url=chosen_worker.url, json=new_task.request_data, headers=new_task.headers) as response:
                data: dict = await response.json()

                async with chosen_worker.lock:
                    chosen_worker.wait_time -= data['real_process_time']

                # 记录任务结束时间
                chosen_worker.finished_cnt += 1
                chosen_worker.processing_cnt -= 1
                # chosen_worker.wait_time -= data['start_process_time'] - manager_received_timestamp
                    
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

                stdout_logger.info(f'{data}\n')
                stdout_logger.info(f"{'Before waiting jobs:':<50}{processing_cnt:<20}\n")
                stdout_logger.info(f"{'worker real wait time:':<50}{data['real_task_wait_time']:<20}\n")
                stdout_logger.info(f"{'worker pred wait time:':<50}{data['pred_task_wait_time']:<20}\n")
                stdout_logger.info(f"{'task pred wait time:':<50}{data['processed_time']}")
                stdout_logger.info(f"{'task pred process time:':<50}{new_task.pred_processed_time}")
                stdout_logger.info(f"{'Datetime:':<50}{datetime.ctime(datetime.now()) :<20}\n")

                stdout_logger.info(f"After {time.time()} Request number {new_task.request_data.get('number')}")
                stdout_logger.info(f"processing_cnt: {chosen_worker.processing_cnt} ")
                stdout_logger.info(f"diff between predict wait time and real wait time: {data['pred_task_wait_time'] - data['real_task_wait_time']}")
                
                return web.json_response(data)

        except Exception:
            error_message = traceback.format_exc()
            print(error_message)
            return web.json_response({"error": error_message, "data": data}, status=500)

    
    async def on_shutdown(self):
        for worker in self.workers:
            worker.stop_event.set()
            if worker.hdd_task:
                await worker.hdd_task
            if worker.mem_task:
                await worker.mem_task
            await worker.close_session()
            
            worker.ssh_client.close()
            
            print("Session connection closed.")
            print("SSH connection closed.")

