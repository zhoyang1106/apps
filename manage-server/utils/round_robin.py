import random
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from collections import deque
import matplotlib.pyplot as plt
import gurobipy as gp
from gurobipy import GRB



# MEM_CAPACITY = 500*1_000_000 # 500 MB (use any number)
# HDD_CAPACITY = 1.5*1_000_000_000 # 1.5GB (use any number)
MEM_CAPACITY = 900*1_000_000 # 900 MB (use any number)       ## 서버의 실제값
HDD_CAPACITY = 14.5*1_000_000_000 # 14.5GB (use any number)   ## 서버의 실제값



def round_robin_assignment(index, Task, WORKERS):
    NUM_WORKERS = 3
    server_index = index % NUM_WORKERS

    worker_index = 0
    for worker in WORKERS:
        if (worker.hdd_usage + Task.hdd_usage <= worker.max_hdd_usage and worker.mem_usage + Task.mem_usage <= worker.max_mem_usage):
            worker_index = server_index

    return worker_index



