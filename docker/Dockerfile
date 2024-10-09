# 使用 arm64 架构的 Python 3.12 slim 版本
FROM arm64v8/python:3.12-slim-bookworm

# 设置环境变量，避免 Python 缓存 .pyc 文件
ENV PYTHONDONTWRITEBYTECODE=1

# 确保 Python 日志立即显示
ENV PYTHONUNBUFFERED=1

# 更新包管理器并安装构建依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libffi-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# 创建工作目录
WORKDIR /app

# 升级 pip 并安装 aiohttp
RUN pip install --upgrade pip
RUN pip install aiohttp psutil

# 复制当前目录的代码到容器中的工作目录
COPY . /app

# 容器启动时运行的默认命令
CMD [ "python3", "app.py" ]
