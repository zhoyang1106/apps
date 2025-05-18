from aiohttp import web
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import psutil  # type: ignore
import os
import time
import traceback
import asyncio


# Global process pool
thread_executor = ThreadPoolExecutor(max_workers=1)
process_executor = ProcessPoolExecutor(max_workers=1)

def get_cpu_times(pid):
    process = psutil.Process(pid)
    cpu_times = process.cpu_times()
    user_times = cpu_times.user
    system_times = cpu_times.system

    return user_times, system_times

def is_prime(num):
    if num <= 1:
        return False
    if num == 2:
        return True
    if num % 2 == 0:
        return False
    for i in range(3, int(num**0.5) + 1, 2):
        if num % i == 0:
            return False
    return True


def prime_count(num, arrival_time):
    start_time = time.perf_counter()
    queue_waiting_time = start_time - arrival_time

    # get child pid
    child_pid = os.getpid()

    start_user_times, start_system_times = get_cpu_times(child_pid)

    sum = 0
    for i in range(2, num):
        if is_prime(i):
            sum += 1

    # fetch cpu data
    end_user_times, end_system_times = get_cpu_times(child_pid)
    user_times_diff = end_user_times - start_user_times
    system_times_diff = end_system_times - start_system_times

    real_process_time = time.perf_counter() - start_time
    contention_time = real_process_time - (user_times_diff + system_times_diff)

    return {
        "request_num": num,
        "task_arrivaled_time": arrival_time,
        "return_result": sum,
        "user_cpu_time": user_times_diff,
        "system_cpu_time": system_times_diff,
        "real_process_time": real_process_time,
        "contention_time": max(0, contention_time),
        "queue_waiting_time": queue_waiting_time,
        "finish_time": time.time()
    }



async def handle(request: web.Request):
    data = await request.json()

    print(f"Get request from {data}")
    arrival_time = time.perf_counter()

    # Step 1: Simulate Queue Waiting Time
    try:
        processing_start_time = time.perf_counter()

        # Run the blocking task
        loop = asyncio.get_event_loop()

        # response_data = await loop.run_in_executor(thread_executor, prime_count, data["number"])
        response_data = await loop.run_in_executor(thread_executor, prime_count, data["number"], arrival_time)
        
        print(f"Send response to 192.168.0.100")
        # logging.info(f"Current Processing Tasks Sum: {PROCESSING_CNT}")
        return web.json_response(response_data, status=200)
    except Exception as e:
        error_message = traceback.format_exc()
        return web.json_response({"error": error_message}, status=500)


def create_app():
    app = web.Application()

    # 添加路由
    app.router.add_post('', handle)

    return app

if __name__ == '__main__':
    app = create_app()
    web.run_app(app, host="0.0.0.0", port=8081)
