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
NUM_WORKERS = 3




'''
Model
'''
def Optimization_Model1(response_time, Task, WORKERS):

    # Create model
    model = gp.Model("server_optimization")

    # Create variables
    b_hdd = model.addVars(1, vtype=GRB.CONTINUOUS, name="b_hdd")
    b_mem = model.addVars(1, vtype=GRB.CONTINUOUS, name="b_mem")
    x = model.addVars(NUM_WORKERS, vtype=GRB.BINARY, name="x")  # x = 0 or 1  이진법

    # Set objective function：
    # weight
    alpha = 1/3
    beta = 1/3
    gamma = 1/3

    for worker in WORKERS:
            # maximum value
            hdd_max = worker.max_hdd_usage
            mem_max = worker.max_mem_usage
    # hdd_max = HDD_CAPACITY
    # mem_max = MEM_CAPACITY
    response_time_max = max(response_time)

    model.setObjective((alpha * (b_hdd[0] / hdd_max)) + (beta * (b_mem[0] / mem_max)) + gamma * (gp.quicksum(response_time[worker.id] * x[worker.id] for worker in WORKERS) / response_time_max), GRB.MINIMIZE)

    # Add constraint
    model.addConstr(gp.quicksum(x[worker.id] for worker in WORKERS) == 1, "c1")
    for worker in WORKERS:
        # maximum value
        model.addConstr(worker.hdd_usage + Task.hdd_usage * x[worker.id] <= worker.max_hdd_usage)
        model.addConstr(worker.mem_usage + Task.mem_usage * x[worker.id] <= worker.max_mem_usage)
        # b_hdd
        model.addConstr(worker.hdd_usage + Task.hdd_usage * x[worker.id] <= b_hdd[0])
        # b_mem
        model.addConstr(worker.mem_usage + Task.mem_usage * x[worker.id] <= b_mem[0])

    # Optimize modeal
    model.optimize()

    # Print results
    # worker_index = 5
    if model.status == GRB.OPTIMAL:
        # 获取所有 x[worker.id].X 的值
        x_values = [x[worker.id].X for worker in WORKERS]
        print(sum(x_values))
        print(x_values)

        # 进行最小-最大归一化
        x_min = min(x_values)
        x_max = max(x_values)
        if x_max > x_min:  # 防止分母为0
            normalized_x_values = [(x_val - x_min) / (x_max - x_min) for x_val in x_values]
        else:
            normalized_x_values = [0 for _ in x_values]  # 如果所有值相等，则归一化为 0

        # 输出归一化结果并确定选定的 worker
        for i, worker in enumerate(WORKERS):
            if normalized_x_values[i] == 1.0:  # 选择归一化后非零的 worker
                worker_index = i
                print(i, worker_index)

    return worker_index





