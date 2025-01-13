# manager_node/run_with_middleware.py

import time
import asyncio
from aiohttp import web
import uvloop

# 从你的主文件中导入 create_app
# 注意：你的 manager_node/main.py 必须有 create_app() 函数对外可用
from main3_middle import create_app


@web.middleware
async def my_middleware(request, handler):
    """示例中间件：在每个请求前后打印日志 & 计算处理时长等。"""
    start_time = time.time()

    # 这里可以做各种前置逻辑，比如鉴权、限流统计、给 request 打标记等等
    print(f"[MIDDLEWARE] Before handling {request.method} {request.rel_url}")

    # 调用下一个处理器（可能是下一个中间件或最终的 handler）
    response = await handler(request)

    # 后置逻辑，比如记录处理耗时
    duration = time.time() - start_time
    print(f"[MIDDLEWARE] After handling {request.method} {request.rel_url}, took {duration:.4f}s")

    return response


def main():
    # 1) 先拿到原先的 app（不做任何修改）
    app = create_app()

    # 2) 向 app 中追加中间件
    #   注意，这不会改动你的 main.py，而是在启动时“动态”插入
    app.middlewares.append(my_middleware)

    # 3) 最后像原来一样运行 app
    #   你可以保持和 main.py 相同的 host/port，也可以换
    web.run_app(app, host='192.168.0.100', port=8199)


if __name__ == "__main__":
    main()
