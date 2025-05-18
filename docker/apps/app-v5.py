from aiohttp import web
import time
import asyncio
import traceback
import psutil
import os
import json

# 新版redis异步库
import redis.asyncio as redis

# Helper to get CPU times
def get_cpu_times(pid):
    process = psutil.Process(pid)
    cpu_times = process.cpu_times()
    user_times = cpu_times.user
    system_times = cpu_times.system
    return user_times, system_times

# Function to check prime number
def is_prime(n):
    if n < 2:
        return n == 2  # 只有2是质数
    if n % 2 == 0:
        return n == 2
    i = 3
    while i * i <= n:
        if n % i == 0:
            return False
        i += 2
    return True

# Count prime numbers
def count_prime_number(n):
    count = 0
    for i in range(n):
        if is_prime(i):
            count += 1
    return count

# Process task and return performance data
def process(n, ma, d):
    start_process_time = time.perf_counter()
    data_prepare_time = time.perf_counter() - d
    waiting_time = time.time() - ma
    worker_start_time = time.time()
    
    # get child pid
    child_pid = os.getpid()

    start_user_times, start_system_times = get_cpu_times(child_pid)

    result = count_prime_number(n)

    # fetch cpu data
    end_user_times, end_system_times = get_cpu_times(child_pid)
    user_times_diff = end_user_times - start_user_times
    system_times_diff = end_system_times - start_system_times

    real_process_time = time.perf_counter() - start_process_time
    contention_time = real_process_time - (user_times_diff + system_times_diff)

    cpu_time_spent = user_times_diff + system_times_diff
    cpu_usage_percent = (cpu_time_spent / real_process_time) * 100

    return {
        "result": result,
        "worker_start_time": worker_start_time,
        "waiting_time": waiting_time,
        "start_process_time": start_process_time,
        "real_process_time": real_process_time,
        "data_prepare_time": data_prepare_time,
        "finish_time": time.perf_counter(),
        "user_cpu_time": user_times_diff,
        "system_cpu_time": system_times_diff,
        "contention_time": contention_time,
        "cpu_spent_usage": cpu_usage_percent,
    }

# Generate response to send back
def response_gen(response: dict):
    start_process_time = response.pop('start_process_time')
    return {
        **response,
        "response_time": response.get('finish_time') - start_process_time,
    }

# Function to process tasks asynchronously
async def process_task_from_redis(redis_client):
    while True:
        # 从Redis的任务队列中获取任务 (阻塞式, 超时时间可自行调整)
        # brpop返回结果类似: ["task_queue", "任务字符串"], 如果取不到会等待
        task_data = await redis_client.brpop('task_queue', timeout=0)  # 等待直到有数据
        if task_data:
            # 注意: redis返回的是 [list_name, value] 的形式
            _, message = task_data
            try:
                request_data = json.loads(message)
            except:
                # 若你推送的不是JSON字符串，请根据实际情况解析
                request_data = {"number": 10000}

            manager_arrival_time: int | None | web.Any = request_data.get('manager_arrival_time')
            data_prepare_start = time.perf_counter()

            response = process(request_data.get('number', 10000),
                               manager_arrival_time,
                               data_prepare_start)


            # 生成要返回给"response_queue"的内容
            resp = response_gen(response)
            
            # rpush时需要push字符串，故用json.dumps
            await redis_client.rpush('response_queue', json.dumps(resp))

        await asyncio.sleep(1)  # 可选：给事件循环一些空闲，按需增减

# 用于启动时初始化 Redis 连接
async def init_app(app):
    # 建立连接池
    # decode_responses=True 可以让返回值直接是字符串而不是字节
    app['redis'] = redis.from_url("redis://192.168.0.100:6379", 
                                  decode_responses=True)
    # 启动一个后台任务，用于持续监听与处理队列任务
    asyncio.create_task(process_task_from_redis(app['redis']))

async def handle(request: web.Request):
    try:
        arrival_time = time.perf_counter()
        request_data = await request.json()

        transmit_delay = time.perf_counter() - arrival_time

        # 把任务推到Redis的任务队列 (用rpush)
        redis_client = request.app['redis']
        # 需要存字符串, 如果是json数据, 用json.dumps
        await redis_client.rpush('task_queue', json.dumps(request_data))

        return web.json_response({
            "message": "Task submitted successfully",
            "transmit_delay": transmit_delay
        })

    except Exception as e:
        error = traceback.format_exc()
        print(error)
        return web.json_response({
            "error": error
        })

async def cleanup(app):
    redis_client = app.get('redis')
    if redis_client:
        await redis_client.close()
        await redis_client.wait_closed()
        print("Redis connection closed.") 

def create_app():
    app = web.Application()
    # 可以根据需要设置更多路由
    app.router.add_post("/submit_task", handle)
    app.on_startup.append(init_app)
    app.on_cleanup.append(cleanup)
    return app

if __name__ == '__main__':
    web.run_app(create_app(), host="0.0.0.0", port=8080)
