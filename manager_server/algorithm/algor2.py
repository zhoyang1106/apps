import random
import time
from datetime import datetime
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from collections import deque
import matplotlib.pyplot as plt
import gurobipy as gp
from gurobipy import GRB










def Optimization_Model2(response_time, Task, WORKERS):
    NUM_WORKERS = len(WORKERS)
    # weight (sum=1, random)
    alpha = 1/3
    beta = 1/3
    gamma = 1/3
    # Maximum value measured by 20000 tests
    hdd_usage_std_max = 0.166666667
    mem_usage_std_max = 0.166666667

    response_time_max = max(response_time)
    
    # Create model
    start_modeling = time.perf_counter_ns()
    model = gp.Model("server_optimization")

    # Create variables
    x = model.addVars(NUM_WORKERS, vtype=GRB.BINARY, name="x")  # x = 0 or 1  이진법

    hdd_usage_std = gp.quicksum(((Task.hdd_usage * x[j] + WORKERS[j].hdd_usage) - (Task.hdd_usage + WORKERS[j].hdd_usage) / NUM_WORKERS) ** 2 for j in range(NUM_WORKERS))
    mem_usage_std = gp.quicksum(((Task.mem_usage * x[j] + WORKERS[j].mem_usage) - (Task.mem_usage + WORKERS[j].mem_usage) / NUM_WORKERS) ** 2 for j in range(NUM_WORKERS))

    # Set objective function：
    model.setObjective(alpha / hdd_usage_std_max * hdd_usage_std + beta / mem_usage_std_max * mem_usage_std + gamma / response_time_max * gp.quicksum(response_time[j] * x[j] for j in range(NUM_WORKERS)), GRB.MINIMIZE)

    # Add constraint
    model.addConstr(gp.quicksum(x[j] for j in range(NUM_WORKERS)) == 1, "c1")
    for j in range(NUM_WORKERS):
        # maximum value
        model.addConstr(WORKERS[j].hdd_usage + Task.hdd_usage * x[j] <= WORKERS[j].max_hdd_usage)
        model.addConstr(WORKERS[j].mem_usage + Task.mem_usage * x[j] <= WORKERS[j].max_mem_usage)
    end_modeling = time.perf_counter_ns()
    modeling_time = end_modeling - start_modeling


    # model.setParam(GRB.Param.Threads, 4)
    
    # Optimize modeal
    start_solving = time.perf_counter_ns()
    model.optimize()
    end_solving = time.perf_counter_ns()
    solver_time = end_solving - start_solving

    # Print results
    if model.status == GRB.OPTIMAL:
        # print("algor2_CPU time:", end_time - start_time)
        # 获取所有 x[worker.id].X 的值
        x_values = [x[j].X for j in range(NUM_WORKERS)]
    return x_values, (solver_time, modeling_time)
    




