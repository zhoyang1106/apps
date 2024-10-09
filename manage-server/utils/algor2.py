import random
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from collections import deque
import matplotlib.pyplot as plt
import gurobipy as gp
from gurobipy import GRB



TIME_SLOT_LENGTH = 1  # ms
SIMULATION_ITER_MAX = 1000 # max number of time slot
TASK_GENERATE_RATE = 30 # one task for three time slots
MEM_CAPACITY = 500*1_000_000 # 500 MB (use any number)
HDD_CAPACITY = 1.5*1_000_000_000 # 1.5GB (use any number)
num_tasks_generated = 0
MAX_NUM_TASKS_TO_GENERATE = 3

# initialize
NUM_WORKERS = 3
task_queues_in_processing = [[] for w in range(NUM_WORKERS)] # tasks that are being processed
tasks_done_processing = []  # tasks that are done processing





def Optimization_Model2(response_time, Task, WORKERS):

    # Create model
    model = gp.Model("server_optimization")

    # Create variables
    # x = model.addVars(num_servers, lb=0.0, ub=1.0, vtype=GRB.CONTINUOUS, name="x")
    x = model.addVars(NUM_WORKERS, vtype=GRB.BINARY, name="x")  # x = 0 or 1  이진법

    # Set objective function：
    # weight (sum=1, random)
    alpha = 1/3
    beta = 1/3
    gamma = 1/3

    # Maximum value measured by 20000 tests
    hdd_usage_std_max = 0.166666667
    mem_usage_std_max = 0.166666667
    response_time_max = max(response_time)

    hdd_usage_std = gp.quicksum(((Task.hdd_usage * x[worker] + worker.hdd_usage) - (Task.hdd_usage + worker.hdd_usage) / NUM_WORKERS) ** 2 for worker in WORKERS)
    mem_usage_std = gp.quicksum(((Task.mem_usage * x[worker] + worker.mem_usage) - (Task.mem_usage + worker.mem_usage) / NUM_WORKERS) ** 2 for worker in WORKERS)

    model.setObjective(alpha / hdd_usage_std_max * hdd_usage_std + beta / mem_usage_std_max * mem_usage_std + gamma / response_time_max * gp.quicksum(response_time[worker] * x[worker] for worker in WORKERS), GRB.MINIMIZE)

    # Add constraint
    model.addConstr(gp.quicksum(x[worker] for worker in WORKERS) == 1, "c1")
    for worker in WORKERS:
    # maximum value
        model.addConstr(worker.hdd_usage + Task.hdd_usage * x[i] <= HDD_CAPACITY, f"c2")
        model.addConstr(worker.mem_usage + Task.mem_usage * x[i] <= MEM_CAPACITY, f"c3")

    # Optimize modeal
    model.optimize()

    # Print results
    if model.status == GRB.OPTIMAL:
        for worker in WORKERS:
            total_load = sum(x[worker].X)
            if x[worker].X / total_load != 0:
                return worker


