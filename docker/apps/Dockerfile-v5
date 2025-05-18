# arm64 Python 3.12 slim 
FROM arm64v8/python:3.12-slim-bookworm

# enviroment variable, prevent Python caches *.pyc file
ENV PYTHONDONTWRITEBYTECODE=1

# sure Python log displayed
ENV PYTHONUNBUFFERED=1

# update
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libffi-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# make worker dir
WORKDIR /app

# update pip
RUN pip install --upgrade pip
RUN pip install psutil redis logging

COPY app-v5.py /app

CMD [ "python3", "-u", "app-v5.py" ]
