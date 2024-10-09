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




'''
Model
'''
def Optimization_Model1(response_time, Task, WORKERS):

    # Create model
    model = gp.Model("server_optimization")

    # Create variables
    b_hdd = model.addVars(1, lb=0.0, ub=1.0, vtype=GRB.CONTINUOUS, name="b_hdd")
    b_mem = model.addVars(1, lb=0.0, ub=1.0, vtype=GRB.CONTINUOUS, name="b_mem")
    x = model.addVars(NUM_WORKERS, vtype=GRB.BINARY, name="x")  # x = 0 or 1  이진법

    # Set objective function：
    # weight
    alpha = 1/3
    beta = 1/3
    gamma = 1/3

    hdd_max = HDD_CAPACITY
    mem_max = MEM_CAPACITY
    response_time_max = max(response_time)

    model.setObjective((alpha * (b_hdd[0] / hdd_max)) + (beta * (b_mem[0] / mem_max)) + gamma * (gp.quicksum(response_time[worker] * x[worker] for worker in WORKERS) / response_time_max), GRB.MINIMIZE)

    # Add constraint
    model.addConstr(gp.quicksum(x[worker] for worker in WORKERS) == 1, "c1")
    for worker in WORKERS:
        # maximum value
        model.addConstr(worker.hdd_usage + Task.hdd_usage * x[worker] <= HDD_CAPACITY, f"c2")
        model.addConstr(worker.mem_usage + Task.mem_usage * x[worker] <= MEM_CAPACITY, f"c3")
        # b_hdd
        model.addConstr(worker.hdd_usage + Task.hdd_usage * x[worker] <= b_hdd[0], f"c4")
        # b_mem
        model.addConstr(worker.mem_usage + Task.mem_usage * x[worker] <= b_mem[0], f"c5")

    # Optimize modeal
    model.optimize()

    # Print results
    if model.status == GRB.OPTIMAL:
        for worker in WORKERS:
            total_load = sum(x[worker].X)
            if x[worker].X / total_load != 0:
                return worker





