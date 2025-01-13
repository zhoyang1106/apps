# Waiting queue on manager before send to backend worker node

# manager_node/main.py
import asyncio
from pathlib import Path
from aiohttp import web, ClientSession, ClientTimeout
from utils import setup_logger, load_xgboost_model
import traceback
import time
import xgboost as xgb

# 假设你的后端 Worker 列表如下
WORKERS= [
    'http://192.168.0.150:8080',
    'http://192.168.0.151:8080',
    'http://192.168.0.152:8080',
]

ROUND_ROBIN_INDEX = 0

def predict_processed_time(request_data: dict):
    data = xgb.DMatrix([[request_data.get('number', 0)]])
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

    if algorithm_name == 'shortest':
        # 选队列里等待时间最少的 worker
        min_finish = float('inf')
        selected_worker = None
        now = time.time()
        for worker_id, finish_t in app['predict_finish_time'].items():
            waiting = max(0, finish_t - now)
            if waiting < min_finish:
                min_finish = waiting
                selected_worker = worker_id
        return selected_worker

    elif algorithm_name == 'round-robin':
        selected_worker = ROUND_ROBIN_INDEX
        ROUND_ROBIN_INDEX = (ROUND_ROBIN_INDEX + 1) % len(WORKERS)
        return selected_worker

    elif algorithm_name == "least-connect":
        # 和 shortest 类似，因“前置队列”其实也是“谁队列短就接收请求少”
        min_queue_size = float('inf')
        selected_worker = 0
        for worker_id, queue in app['request_queues'].items():
            qsize = queue.qsize()
            if qsize < min_queue_size:
                min_queue_size = qsize
                selected_worker = worker_id
        return selected_worker

    elif algorithm_name == "min-entropy":
        selected_worker = max(app['dynamic_weight'], key=app['dynamic_weight'].get)
        return selected_worker

    else:
        # 未指定算法时，默认 worker 0
        return 0

def update_weights(app, process_time, worker_id):
    alpha = 1.0     # complex weight
    beta = 0.2      # quene size weight

    task_counts = [queue.qsize() for queue in app['request_queues'].values()]
    new_weight = max(0, 1 / (1 + alpha * process_time + task_counts[worker_id] * beta) - app['dynamic_weight'][worker_id])

    weights = []

    app['dynamic_weight'][worker_id] = new_weight
    for n, t in zip(task_counts, app['dynamic_weight'].values()):
        weights.append(1 / (1 + alpha * t + n * beta))

    total = sum(weights)
    for index, weight in enumerate(weights):
        app['dynamic_weight'][index] = weight / total


async def worker_consumer(app, worker_id):
    session = app['sessions'][worker_id]
    queue = app['request_queues'][worker_id]
    backend_url = WORKERS[worker_id]

    while True:
        # 取队列里的下一个请求
        # 如果队列为空，这里将阻塞直到新的请求进入
        item = await queue.get()
        request_data = item['request_data']
        future = item['future']
        request_id = item.get('request_id', 'unknown')

        try:
            # 发送给后端
            async with session.post(backend_url, json=request_data) as resp:
                if resp.status != 200:
                    raise Exception(f"Worker node {worker_id} returned status {resp.status}")

                response_data = await resp.json()


                # 将后端返回的内容设置给 future
                future.set_result(response_data)

        except Exception as e:
            # 如果出错了，也要将异常通过 future 抛回给请求端
            future.set_exception(e)

        finally:
            # 标记队列中的这个任务处理结束
            queue.task_done()


async def handle(request: web.Request):
    receive_time = time.time()
    try:
        request_data: dict = await request.json()
        request_id = request_data.get('request_id', None)
        algorithm_name = request.headers.get('algorithm_name', 'shortest')


        # now = time.time()

        # 1) 选定 worker
        worker_id = select_worker(request.app, algorithm_name)

        # 2) 可选：用 XGBoost 预测处理时长，仅用于日志/分析
        predict_time = predict_processed_time(request_data)


        # finish_t = request.app['predict_finish_time'][worker_id]

        # pending_time_estimated = max(0, finish_t - now)


        # new_finish_time = max(finish_t, now) + predict_time
        # request.app['predict_finish_time'][worker_id] = new_finish_time

        stdout_logger.info(
            f"Request {request_id} chosen worker {worker_id} by '{algorithm_name}', "
            f"predict_time={predict_time:.2f}"
        )


        # 3) 为本次请求创建一个 future
        loop = asyncio.get_event_loop()
        future = loop.create_future()

        # 4) 将请求数据 和 future 一起放到该 worker 的队列中
        await request.app['request_queues'][worker_id].put({
            'request_data': request_data,
            'request_id': request_id,
            'future': future,
            'queue_enter_time': receive_time
        })

        if algorithm_name == 'min-entropy':
            request.app['processing_tasks_sum'][worker_id] += 1
            update_weights(request.app, worker_id=worker_id, process_time=predict_time)
            stdout_logger.info(f"[REQUEST] {request_id} Weight updated.")
            stdout_logger.info(f"[WEIGHT LIST]: [{[w for w in request.app['dynamic_weight'].values()]}]")

        # 5) 等待该请求在 worker 消费协程里处理完
        response_data = await future

        # if algorithm_name == 'shortest':
        #     difference = predict_time - (response_data['start_process_time'] - receive_time)
        #     if difference != 0:
        #         current_ft = request.app['predict_finish_time'][worker_id]
        #         now2 = time.time()
        #         # 仅当 current_ft 还没落后于现在，才有意义去改
        #         if current_ft > now2:
        #             new_ft = current_ft - difference
        #             if new_ft < now2:
        #                 new_ft = now2
        #             request.app['predict_finish_time'][worker_id] = new_ft
        #             stdout_logger.info(
        #                 f"[CORRECT] Worker {worker_id}, old_ft={current_ft:.2f}, new_ft={new_ft:.2f}, diff={difference:.2f}"
        #             )

        if algorithm_name == 'min-entropy':
            request.app['processing_tasks_sum'][worker_id] -= 1
            


        response_data = response_data | {
            "worker": worker_id,
            "status": 200,
            # "calculate_wait_time": pending_time_estimated,
            "real_wait_time": response_data['start_process_time'] - receive_time,
            "predict_process_time": predict_time,
        }

        # 6) 返回给客户端
        return web.json_response(response_data)

    except Exception as e:
        err_msg = traceback.format_exc()
        stdout_logger.error(f"Error in request handler: {err_msg}")
        return web.json_response({"error": "Failed to process request"}, status=500)


async def correct_finish_time_periodically(app, interval=1.0):
    """
    周期性地将“已经过期”的 predict_finish_time 拉回当前时间
    避免出现 Worker 早就空闲，却一直顶着很大的 future finish_time
    """
    try:
        while True:
            now = time.time()
            for wid in range(len(WORKERS)):
                ft = app['predict_finish_time'][wid]
                if ft < now:
                    app['predict_finish_time'][wid] = now
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        stdout_logger.info("correct_finish_time_periodically was cancelled.")



async def on_startup(app):
    """
    应用启动时：
    1. 初始化每个 worker 的队列
    2. 初始化 session
    3. 启动每个 worker 的“消费协程”
    """
    # 1) 为每个 worker 准备一个独立队列
    app['request_queues'] = {i: asyncio.Queue() for i in range(len(WORKERS))}

    # 2) 为每个 worker 创建一个共享 session
    app['sessions'] = {
        i: ClientSession(timeout=ClientTimeout(None)) for i in range(len(WORKERS))
    }

    # 3) 启动 worker 的“消费协程”
    app['worker_tasks'] = []
    for worker_id in range(len(WORKERS)):
        task = asyncio.create_task(worker_consumer(app, worker_id))
        app['worker_tasks'].append(task)

    app['processing_tasks_sum'] = {i: 0 for i in range(len(WORKERS))}
    app['cumulative_times'] = {i: 0 for i in range(len(WORKERS))}
    app['dynamic_weight'] = {i: 1 / len(WORKERS) for i in range(len(WORKERS))}

    now = time.time()
    app['predict_finish_time'] = {i: now for i in range(len(WORKERS))}

    app['correct_ft_task'] = asyncio.create_task(correct_finish_time_periodically(app, interval=1))

    stdout_logger.info("Application startup: all worker queues and consumer tasks are ready.")


async def on_cleanup(app):
    # 1) 取消消费协程
    for task in app['worker_tasks']:
        task.cancel()
    await asyncio.gather(*app['worker_tasks'], return_exceptions=True)

    # 2) 取消定时任务
    app['correct_ft_task'].cancel()
    await asyncio.gather(app['correct_ft_task'], return_exceptions=True)

    # 3) 关闭 sessions
    for session in app['sessions'].values():
        await session.close()

    stdout_logger.info("All worker tasks canceled and client sessions closed.")


# ========== 初始化日志、模型、app ==========

stdout_logger, diff_graph_logger, chrono_logger = setup_loggers()
xgboost_model = load_xgboost_model()

def create_app():
    app = web.Application()
    app.router.add_post('', handle)  # 等效于 POST 到 '/'
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app

if __name__ == '__main__':
    web.run_app(create_app(), host='192.168.0.100', port=8199)
