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
import threading
import queue

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
        self.id = kwargs.get("id", f'x{random.randint(0, 1000)}')
        
        
        # self.lock = asyncio.Lock()
        self.lock = threading.Lock()

        # self.timer_lock = multiprocessing.Lock()
        
        # self.wait_time_sync_queue = queue.Queue(maxsize=1)

        self.wait_time = 0.0
        self.wait_time_update_interval = kwargs.get('update_interval')
        self.wait_time_update_process = None

        # every status tasks sum on worker
        self.processing_cnt = 0
        self.received_cnt = 0
        self.finished_cnt = 0

        self.stop_event = asyncio.Event()

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

        self.hdd_task = None
        self.mem_task = None

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
        while self.stop_event:
            # with self.lock:
            self.wait_time -= self.wait_time_update_interval
                # self.wait_time_sync_queue.put(self.wait_time)
            time.sleep(self.wait_time_update_interval)



    def run_ssh_command(self, cmd: str):
        try:
            if self.ssh_client:
                stdin, stdout, stderr = self.ssh_client.exec_command(command=cmd)
                output = stdout.read().decode()
                return output
            else:
                return None
        except:
            return None

    def hdd_output_parse(self, output: str):
        total_blocks = 0
        if output:
            for line in output.splitlines():
                parts = line.split()
                if len(parts) > 0 and parts[0].startswith('/dev'):
                    blocks = int(parts[1])
                    total_blocks += blocks

            return total_blocks
        else:
            return None

    def mem_output_parse(self, output: str):
        if output:
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
        else:
            return None

    def hdd_usage_handle(self):
        cmd = """df -B"""
        try:
            while not self.stop_event.is_set():
                output = self.run_ssh_command(cmd=cmd)
                # 处理逻辑
                total_blocks = self.hdd_output_parse(output)
                if total_blocks:
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
                mem_usage = self.mem_output_parse(output)
                if mem_usage:
                    self.mem_usage = mem_usage

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
        self.loggers: typing.List[logging.Logger] = kwargs.get('loggers', [])
        self.ws_workers = typing.Dict[str, web.WebSocketResponse or None] = { worker.ip: None for worker in self.workers }
        print("SSH connect started")

    async def start_worker_hdd_mem_task(self):
        loop = asyncio.get_running_loop()
        for worker in self.workers:
            worker.hdd_task = loop.run_in_executor(None, worker.hdd_usage_handle)
            worker.mem_task = loop.run_in_executor(None, worker.mem_usage_handle)

            
    
    async def start_worker_update_time_process(self):
        loop = asyncio.get_running_loop()
        for worker in self.workers:
            worker.wait_time_update_process = loop.run_in_executor(None, worker.wait_time_update)


    def predict_processed_time(self, task: Task):
        data = xgb.DMatrix([[task.request_data.get('number')]])
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
                    # with chosen_worker.lock:
                    chosen_worker.wait_time = 0
                    break
            
            if not chosen_worker:
                algorithm_name = new_task.headers.get('algo_name')
                chosen_worker = await self.choose_worker(name=algorithm_name, new_task=new_task)

            if not chosen_worker:
                raise Exception("chosen worker is None")

            new_task.worker_id = chosen_worker.id
            # with chosen_worker.lock:
            # if chosen_worker.wait_time_sync_queue.full():
                # new_task.wait_time = chosen_worker.wait_time_sync_queue.get_nowait()
            new_task.wait_time = chosen_worker.wait_time
            chosen_worker.wait_time += new_task.pred_processed_time
            # chosen_worker.wait_time_sync_queue.put(chosen_worker.wait_time)  

            chosen_worker.processing_cnt += 1
                
            return chosen_worker, new_task
        except Exception as e:
            error_message = traceback.format_exc()
            print(error_message)
            self.loggers[0].error(error_message)
            exit(1)

    async def start_sessions(self):
        for worker in self.workers:
            await worker.start_session()



    async def manager_ws_handler(request: web.Request, ):
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        worker_id = request.query.get("worker_id", "unknown-worker")
        # connceted_workers[worker_id] = ws


    # handle request main function
    async def request_handler(self, request: web.Request):
        try:
            # received time
            manager_received_timestamp = time.time()
            self.loggers[0].info(f"received time: {manager_received_timestamp}\n")

            # generate task and put into manager tasks queue
            request_data = await request.json()
            chosen_worker, new_task = await self.handle_new_task(request_data, request.headers)
            
            processing_cnt = chosen_worker.processing_cnt
            # fetch queue first task and send
            
            self.loggers[0].info(f'{"-" * 40}\n')
            self.loggers[0].info(f"Before {time.time()} Request number {new_task.request_data.get('number')}")
            self.loggers[0].info(f"task prediction process time {new_task.pred_processed_time}")
            self.loggers[0].info(f"processing_cnt: {chosen_worker.processing_cnt}")
            self.loggers[0].info(f"task wait time: {new_task.wait_time}")
            self.loggers[0].info(f"total predict response time: {new_task.wait_time + new_task.pred_processed_time}")


            total_response_time_prediction = new_task.wait_time + new_task.pred_processed_time
            before_forward_timestamp = time.time()
            before_forward_time = before_forward_timestamp - manager_received_timestamp


            # send this task to worker node
            async with chosen_worker.session.post(url=chosen_worker.url, json=new_task.request_data, headers=new_task.headers) as response:
                data: dict = await response.json()


                # test 1: 直接赋值
                # with chosen_worker.lock:
                    # chosen_worker.wait_time = data['pred_task_wait_time']

                # test 2: 使用差值消除
                # with chosen_worker.lock:
                chosen_worker.wait_time -= new_task.wait_time - (data['start_process_time'] - manager_received_timestamp)

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

                self.loggers[0].info(f'{"-" * 40}\n')
                self.loggers[0].info(f'{data}\n')
                self.loggers[0].info(f"{'Before waiting jobs:':<50}{processing_cnt - 1:<20}\n")
                self.loggers[0].info(f"{'worker real wait time:':<50}{data['real_task_wait_time']:<20}\n")
                self.loggers[0].info(f"{'worker pred wait time:':<50}{data['pred_task_wait_time']:<20}\n")
                self.loggers[0].info(f"{'task pred wait time:':<50}{data['processed_time']}")
                self.loggers[0].info(f"{'task pred process time:':<50}{new_task.pred_processed_time}")
                self.loggers[0].info(f"{'Datetime:':<50}{datetime.ctime(datetime.now()) :<20}\n")

                self.loggers[0].info(f"After {time.time()} Request number {new_task.request_data.get('number')}")
                self.loggers[0].info(f"processing_cnt: {chosen_worker.processing_cnt} ")
                self.loggers[0].info(f"diff between predict wait time and real wait time: {data['pred_task_wait_time'] - data['real_task_wait_time']}")
                
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
                worker.hdd_task = None
            if worker.mem_task:
                await worker.mem_task
                worker.hdd_task = None
            await worker.close_session()

            await worker.wait_time_update_process
            
            
            print("Session connection closed.")
            print("SSH connection closed.")
            print("Wait time updater closed.")

