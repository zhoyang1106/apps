import asyncio
import random
from concurrent.futures import ProcessPoolExecutor
import paramiko
import requests
import traceback
import aiohttp
import time
import datetime
from logging import Logger

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


        # logger
        self.stdout_logger: Logger = kwargs.get('logger', None)

        
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
        self.ssh_executor = ProcessPoolExecutor()

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
        self.sum_request_number = 0
        self.worker_load_limit = kwargs.get('cpu_limit')


        # sync Queue
        self.tasks_queue = asyncio.Queue()
        self.results_queue = asyncio.Queue()


    # sync handle task


    async def start_processing_tasks(self):
        """Start the task processing coroutine."""
        asyncio.create_task(self._process_tasks())


    async def _process_tasks(self):
        while True:
            try:
                # 从队列中取出任务
                task: Task = await self.tasks_queue.get()
                manager_received_timestamp = time.time()

                if task is None:  # 特殊信号用于终止线程
                    break
                
                # 模拟发送任务到指定 URL
                async with self.session.post(url=self.url, json=task.request_data, headers=task.headers) as response:
                    data: dict = await response.json()

                    if "error" in data.keys():
                        data["success"] = 0
                    else:
                        data["success"] = 1

                    # update response data
                    data["chosen_ip"] = self.ip
                    data['processed_time'] = data.pop("real_process_time")
                    data['jobs_on_worker_node'] = self.processing_cnt
                    data['total_response_time_prediction'] = task.wait_time + task.pred_processed_time
                    data['real_task_wait_time'] =  data['start_process_time'] - manager_received_timestamp
                    data['pred_task_wait_time'] = task.wait_time

                    self.stdout_logger.info(f'{data}\n')
                    self.stdout_logger.info(f"{'Before waiting jobs:':<50}{self.processing_cnt:<20}\n")
                    self.stdout_logger.info(f"{'worker real wait time:':<50}{data['real_task_wait_time']:<20}\n")
                    self.stdout_logger.info(f"{'worker pred wait time:':<50}{data['pred_task_wait_time']:<20}\n")
                    self.stdout_logger.info(f"{'task pred wait time:':<50}{data['processed_time']}")
                    self.stdout_logger.info(f"{'task pred process time:':<50}{task.pred_processed_time}")
                    self.stdout_logger.info(f"{'Datetime:':<50}{datetime.ctime(datetime.now()) :<20}\n")

                    self.stdout_logger.info(f"After {time.time()} Request number {task.request_data.get('number')}")
                    self.stdout_logger.info(f"processing_cnt: {self.processing_cnt} ")
                    self.stdout_logger.info(f"diff between predict wait time and real wait time: {data['pred_task_wait_time'] - data['real_task_wait_time']}")

                await self.results_queue.put(data)

                with self.lock:
                    self.processing_cnt -= 1
                    self.finished_cnt += 1
                    self.wait_time -= data.get('real_process_time', 0)
                
                # 日志记录
                print(f"Task sent to {self.url}, response: {data}")

            except Exception as e:
                print(f"Error processing task on Worker {self.id}: {e}")

    async def add_task(self, task):
        """向队列中添加任务"""
        async with self.lock:
            self.processing_cnt += 1
        await self.tasks_queue.put(task)


    async def get_result(self):
        """获取处理结果"""
        try:
            return await asyncio.wait_for(self.results_queue.get(), timeout=None)  # 阻塞等待结果，超时为 10 秒
        except asyncio.TimeoutError:
            return {"success": 0, "error": "Timeout waiting for worker result"}

    async def stop_worker(self):
        """发送终止信号到任务队列"""
        await self.tasks_queue.put(None)



    # lowest-score alogrithm (banched)


    async def update_score(self, new_task: Task): # type: ignore
        self.worker_load_score = (self.processing_cnt * 0.01 + (self.sum_request_number + new_task.request_data['number']) * 0.01) / self.worker_load_limit



    # session switch


    async def start_session(self):
        self.session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=0), timeout=aiohttp.ClientTimeout(total=None))

    async def close_session(self):
        await self.session.close()
        self.ssh_client.close()




    # wait time updater


    async def wait_time_update(self):
        while True:
            async with self.lock:
                self.wait_time -= self.wait_time_update_interval
            await asyncio.sleep(0)






    ## ssh


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

    async def hdd_usage_handle(self):
        cmd = """df -B"""
        try:
            while not self.stop_event.is_set():
                output = self.run_ssh_command(cmd=cmd)
                # 处理逻辑
                total_blocks = self.hdd_output_parse(output)
                self.hdd_usage = total_blocks

                await asyncio.sleep(1)
        except Exception as e:
            # 捕获其他异常
            print(e)
            error_message = traceback.format_exc()
            print(f"Error in HDD monitoring task: {error_message}")
            exit(1)
            

    async def mem_usage_handle(self):
        cmd = """cat /proc/meminfo"""
        try:
            while not self.stop_event.is_set():
                output = self.run_ssh_command(cmd=cmd)
                # 处理逻辑
                self.mem_usage = self.mem_output_parse(output)

                await asyncio.sleep(1)
        except Exception as e:
            print(e)
            # 捕获其他异常
            error_message = traceback.format_exc()
            print(f"Error in Memory monitoring task: {error_message}")
            exit(1)
