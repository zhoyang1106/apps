import aiohttp
from aiohttp import web
import time
from datetime import datetime
import traceback
import asyncio
from asyncio import Queue
import logging
from pathlib import Path
import random
from xgboost import Booster
import xgboost
import paramiko
import random
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from collections import deque
import matplotlib.pyplot as plt
import algorithm.dqn as dqn
import algorithm.algor1 as algor1
import algorithm.algor2 as algor2
import algorithm.round_robin as round_robin 



TIME_SLOT_LENGTH = 1  # 단위: ms
SIMULATION_ITER_MAX = 1000 # max number of time slot
TASK_GENERATE_RATE = 30 # one task for three time slots
# MEM_CAPACITY = 500*1_000_000 # 500 MB (use any number)
# HDD_CAPACITY = 1.5*1_000_000_000 # 1.5GB (use any number)
MEM_CAPACITY = 900*1_000_000 # 500 MB (use any number)       ## 서버의 실제값
HDD_CAPACITY = 14.5*1_000_000_000 # 1.5GB (use any number)   ## 서버의 실제값
num_tasks_generated = 0 
MAX_NUM_TASKS_TO_GENERATE = 3

# initialize
NUM_WORKERS = 3
task_queues_in_processing = [[] for w in range(NUM_WORKERS)] # tasks that are being processed
tasks_done_processing = []  # tasks that are done processing


# xgboost model
process_model_path = ("/home/pi/apps/manage-server/models/xgb_number_time.json")
xgboost_proc_model = Booster()
xgboost_proc_model.load_model(process_model_path)


# LOG file
log_path = Path.cwd() / 'log' / f"{__file__}.log"
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
        # record how long time until receive response  응답을 받을 때까지의 시간 기록
        self.until_response_time = 0

        # resource usage
        self.hdd_usage = 0
        self.mem_usage = 0

        # 누적 보상
        self.reward = 0
        self.opt_time = 0


    # xgboost를 사용해 처리 시간을 예측합니다.
    def predict_processed_time(self):
        data = xgboost.DMatrix([[self.request_data.get('number')]])
        prediction = xgboost_proc_model.predict(data)
        return float(prediction[0])

class Worker:
    def __init__(self, **kwargs):
        self.id = kwargs.get('id')  # 服务器 ID
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

        self.max_hdd_usage = 1
        self.max_mem_usage = 1

        # session for worker connector
        self.session = None
 

        # SSH 클라이언트 설정
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


    # 하드 디스크 사용량 처리
    async def hdd_usage_handle(self):
        cmd = """df -B1"""
        try:
            while True:
                stdin, stdout, stderr = self.ssh_client.exec_command(command=cmd)
                output = stdout.read().decode()
                # print("Output of [ df -B1 ]: ", output)

                total_blocks = 0
                current_blocks = 0
                for line in output.splitlines():
                    parts = line.split()
                    if len(parts) > 0 and parts[0].startswith('/dev'):
                        total_blocks += int(parts[1])
                        current_blocks += int(parts[2])
                self.hdd_usage = current_blocks
                self.max_hdd_usage = total_blocks
                
                await asyncio.sleep(0.3)
        
        # Stop with error or interrupt # 오류 또는 중단 발생 시 종료
        except asyncio.CancelledError:
            print("Task was cancelled.")
            raise

        except Exception as e:
            error_message = traceback.format_exc()
            print(error_message)
            exit(1)
        finally:
            self.ssh_client.close()


    # 메모리 사용량 처리
    async def mem_usage_handle(self):
        cmd = """cat /proc/meminfo"""
        try:
            while True:
                stdin, stdout, stderr = self.ssh_client.exec_command(command=cmd)
                output = stdout.read().decode()
                
                # /proc/meminfo 출력에서 메모리 사용량 추출
                mem_info = {}
                for line in output.splitlines():
                    parts = line.split(':')
                    if len(parts) == 2:
                        key = parts[0].strip()
                        value = parts[1].strip().split()[0]  # 값 추출, 단위는 KB
                        mem_info[key] = int(value) * 1024  # KB를 byte로 변환

                mem_total = mem_info.get('MemTotal', 0)
                mem_free = mem_info.get('MemFree', 0)
                mem_available = mem_info.get('MemAvailable', 0)
                mem_used = mem_total - mem_free

                # print(f"Total Memory: {mem_total} bytes")
                # print(f"Free Memory: {mem_free} bytes")
                # print(f"Available Memory: {mem_available} bytes")
                # print(f"Used Memory: {mem_used} bytes")

                 # 300ms 대기 후 다시 체크
                self.mem_usage = mem_used
                self.max_mem_usage = mem_total
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

UPDATE_INTERVAL = 0.001
 

# 3대 서버
WORKERS = [
    Worker(id=0, ip='192.168.0.150', port=8080, update_interval=UPDATE_INTERVAL),
    Worker(id=1, ip='192.168.0.151', port=8080, update_interval=UPDATE_INTERVAL),
    Worker(id=2, ip='192.168.0.152', port=8080, update_interval=UPDATE_INTERVAL),
]




ROUND_ROUBIN_WORKER_INDEX = 0

EPISODE = 0
EPISODE_ADD_CHECK = -1


async def start_sessions():
    global WORKERS
    for worker in WORKERS:
        await worker.start_session()



async def sum_proccessing_cnt():
    global EPISODE_ADD_CHECK
    while True:
        EPISODE_ADD_CHECK = sum([ worker.processing_cnt for worker in WORKERS ])
        await asyncio.sleep(0)
    



# multi IP addresses   다중 IP 주소를 위한 작업자 선택 알고리즘
def choose_url_algorithm(name=None, **kwargs):
    global ROUND_ROUBIN_WORKER_INDEX, EPISODE, num_tasks_generated
    worker_names_obj = kwargs.get('worker_names_obj', None)
    if not worker_names_obj:
        worker_names_obj = WORKERS
    print(worker_names_obj)
    # use params from kwargs
    new_task: Task = kwargs.get('new_task')
    response_time = [ worker.wait_time + new_task.pred_processed_time for worker in worker_names_obj ]

    if not name or name == 'round-robin': # round robin 알고리즘
        worker_index = round_robin.round_robin_assignment(num_tasks_generated, new_task, worker_names_obj)      ## ROUND_ROUBIN_WORKER_INDEX
        # ROUND_ROUBIN_WORKER_INDEX = (ROUND_ROUBIN_WORKER_INDEX + 1) % len(WORKERS)
        return worker_names_obj[worker_index]
    
    elif name == 'dqn':    # DQN 알고리즘
        epsilon = dqn.epsilon_start
        # choose worker
        worker_index = dqn.DQN_Model(response_time, new_task, dqn.epsilon_start, worker_names_obj)
        reward = dqn.get_reward(response_time[worker_index], worker_index)  # 긍정적인 보상 함수 사용
        new_task.reward += reward  # 보상 누적

        epsilon = max(dqn.epsilon_end, epsilon * dqn.epsilon_decay)

        if EPISODE_ADD_CHECK == 0:
            EPISODE += 1

            if EPISODE % dqn.target_update == 0:
                dqn.update_target_network()
                EPISODE = 0

        return worker_names_obj[worker_index]

    elif name == 'algor1':    # 알고리즘 1
        # choose worker
        worker_index = algor1.Optimization_Model1(response_time, new_task, worker_names_obj)
        return worker_names_obj[worker_index]

    else:
        if name == 'algor2':  # 알고리즘 2  (분배 비율의 표준평차 최소화)
            # choose worker
            worker_index = algor2.Optimization_Model2(response_time, new_task, worker_names_obj)
            return worker_names_obj[worker_index]
        
        


# 새 task 처리
async def handle_new_task(request_data: dict, headers: dict):
    global WORKERS, num_tasks_generated

    worker_names: list[Worker] = request_data.get('worker_names')
    worker_names_obj = None

    if worker_names ==  "WORKERS1":
        worker_names_obj = WORKERS[:1]
    elif worker_names ==  "WORKERS2":
        worker_names_obj = WORKERS[:2]
    else:
        worker_names =  "WORKERS"
        worker_names_obj = WORKERS

    # CPU task memory and hard disk (TEST DATA)
    new_task = Task(request_data=request_data, headers=headers)
    if request_data['number'] <= 1000:
        new_task.mem_usage = 176
    elif request_data['number'] > 1000 and request_data['number'] <= 10000:
        new_task.mem_usage = 204
    else:
        new_task.mem_usage = 240
    new_task.hdd_usage = 0
    
    
    # set new_task params
    # ...
    # ...
    # predict process time
    
    # algorithm 
    try:
        chosen_worker = None
        
        for worker in worker_names_obj:
            if worker.processing_cnt == 0:
                chosen_worker = worker
                break

        if not chosen_worker:
            start_time = time.time()
            chosen_worker = choose_url_algorithm(name=request_data['algo_name'], new_task=new_task, worker_names_obj=worker_names_obj)
            end_time = time.time()
            new_task.opt_time = end_time - start_time
           
           
        if not chosen_worker:
            raise Exception("chosen worker is None")

       
        new_task.worker = chosen_worker
        new_task.target_url = chosen_worker.url
        new_task.serving_worker_number = chosen_worker.id
        
        new_task.wait_time = chosen_worker.wait_time
        
        chosen_worker.current_task = new_task

        # add waiting time
        # 예측 처리 시간을 wait_time에 추가

        async with chosen_worker.lock:
            chosen_worker.processing_cnt += 1

        # put into worker queue
        await chosen_worker.tasks_queue.put(new_task)

        return chosen_worker, new_task

    except Exception as e:
        error_message = traceback.format_exc()
        print(error_message)
        exit(1)
    

# handle request main function  # 요청 처리 메인 함수
async def request_handler(request: web.Request):
    global num_tasks_generated
    num_tasks_generated += 1
    try:
        # received time
        manager_received_timestamp = time.time()
        logging.info(f"received time: {manager_received_timestamp}\n")

        # generate task and put into manager tasks queue    task 생성 및 매니저 task 큐에 넣기
        request_data = await request.json()
        chosen_worker, new_task = await handle_new_task(request_data, request.headers)
        response_time = { worker.id: worker.wait_time + new_task.pred_processed_time for worker in WORKERS }
        
        # record data 
        # output.update(information_list, num_tasks_generated, chosen_worker.id, WORKERS, response_time[chosen_worker.id], new_task.hdd_usage, new_task.mem_usage)


        processing_cnt = chosen_worker.processing_cnt
        # fetch queue first task and send   큐의 첫 번째 task 가져오기 및 전송
        
        print('-' * 40, end='\n')
        print(f"Chosen worker: {dir(chosen_worker)}")
        print("Before", time.time(), f"Request number {new_task.request_data.get('number')}")
        print(f"task prediction process time {new_task.pred_processed_time}")
        print(f"worker node nummber:{new_task.serving_worker_number}")
        print("processing_cnt:", chosen_worker.processing_cnt)
        print("task wait time", new_task.wait_time)
        print("total predict response time", new_task.wait_time + new_task.pred_processed_time)
        total_response_time_prediction = new_task.wait_time + new_task.pred_processed_time
        before_forward_timestamp = time.time()
        before_forward_time = before_forward_timestamp - manager_received_timestamp



        await chosen_worker.tasks_queue.get()

        # send this task to worker node  
        async with chosen_worker.session.post(url=chosen_worker.url, json=new_task.request_data, headers=new_task.headers) as response:
            data: dict = await response.json()

            # task 종료 시간을 기록

            async with chosen_worker.lock:
                chosen_worker.finished_cnt += 1
                chosen_worker.processing_cnt -= 1
            if "error" in data.keys():
                data["success"] = 0
            else:
                data["success"] = 1

            # update response datas
            data["chosen_ip"] = chosen_worker.ip
            data['processed_time'] = data.pop("real_process_time")
            data['jobs_on_worker_node'] = processing_cnt
            data['total_response_time_prediction'] = total_response_time_prediction
            data['real_task_wait_time'] =  data['start_process_time'] - manager_received_timestamp
            data['before_forward_time'] = before_forward_time
            data['pred_task_wait_time'] = new_task.wait_time
            data['before_forward_timestamp'] = before_forward_timestamp
            data['rewards'] = new_task.reward
            data['information_list'] = {
                'Worker_index': chosen_worker.id, 
                # 'Server1_hdd_usage': WORKERS[0].hdd_usage,
                # 'Server2_hdd_usage': WORKERS[1].hdd_usage,
                # 'Server3_hdd_usage': WORKERS[2].hdd_usage,
                # 'Server1_mem_usage': WORKERS[0].mem_usage,
                # 'Server2_mem_usage': WORKERS[1].mem_usage,
                # 'Server3_mem_usage': WORKERS[2].mem_usage,
                'Response_time': response_time[chosen_worker.id],
                'Task_hdd_usage': new_task.hdd_usage,
                'Task_mem_usage': new_task.mem_usage,
                'Task_id': num_tasks_generated,
                'Task_opt_time': new_task.opt_time
            }
            
            
            logging.info(f'{"-" * 40}\n')
            logging.info(f'{data}\n')
            logging.info(f"{'Before waiting jobs:':<50}{processing_cnt:<20}\n")
            logging.info(f"{'worker wait time:':<50}{data['real_task_wait_time']:<20}\n")
            logging.info(f"{'Datetime:':<50}{datetime.ctime(datetime.now()) :<20}\n")

            print('-' * 40, end='\n')
            print("After", time.time(), f"Request number {new_task.request_data.get('number')}")
            print(f"worker node nummber:{new_task.serving_worker_number}")
            print("processing_cnt:", chosen_worker.processing_cnt)
            
            return web.json_response(data)

    except Exception:
        error_message = traceback.format_exc()
        print(error_message)
        return web.json_response({"error": error_message, "data": data}, status=500)


# 서버 종료 시 처리 함수
async def on_shutdown(app):
    global WORKERS
    for worker in WORKERS:
        await worker.close_session()

# 서버 애플리케이션 초기화
async def server_app_init():
    global WORKERS
    for worker in WORKERS:
        asyncio.create_task(worker.hdd_usage_handle())
        asyncio.create_task(worker.mem_usage_handle())
        asyncio.create_task(worker.update_wait_time())

    asyncio.create_task(sum_proccessing_cnt())


    print("SSH connect started")
    print("Workers' timer has started")

    app = web.Application()
    app.router.add_post("", request_handler)
    app.on_startup.append(lambda app: start_sessions())
    app.on_cleanup.append(on_shutdown)

    return app


# 서버 실행
def server_run():
    try:
        app = server_app_init()
        web.run_app(app, host='0.0.0.0', port=8100)
    except Exception as e:
        print(f"[ {datetime.ctime(datetime.now())} ]")
        error_message = traceback.format_exc()
        print(error_message)


if __name__ == "__main__":
    server_run()
    
