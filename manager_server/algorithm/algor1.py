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








'''
Model
'''
def Optimization_Model1(response_time, Task, WORKERS):
    NUM_WORKERS = len(WORKERS)
    # weight
    alpha = 1/3
    beta = 1/3
    gamma = 1/3

    hdd_max = max(worker.max_hdd_usage for worker in WORKERS)
    mem_max =  max(worker.max_mem_usage for worker in WORKERS)
    response_time_max = max(response_time)
        
    # Create model
    start_modeling = time.perf_counter_ns()
    model = gp.Model("server_optimization")
   
    # 设置日志输出文件
    # model.setParam("LogFile", "single_task_gurobi_solver.log")
    
    # Create variables
    x = model.addVars(NUM_WORKERS, vtype=GRB.BINARY, name="x")  # x = 0 or 1  이진법
    b_hdd = model.addVars(1, vtype=GRB.CONTINUOUS, name="b_hdd")
    b_mem = model.addVars(1, vtype=GRB.CONTINUOUS, name="b_mem")

    # Set objective function：
    model.setObjective((alpha * (b_hdd[0] / hdd_max)) + (beta * (b_mem[0] / mem_max)) + gamma * (gp.quicksum(response_time[j] * x[j] for j in range(NUM_WORKERS)) / response_time_max), GRB.MINIMIZE)

    # Add constraint
    model.addConstr(gp.quicksum(x[j] for j in range(NUM_WORKERS)) == 1, "c1")
    # start_time_2 = time.perf_counter_ns()
    for j in range(NUM_WORKERS):
        # maximum value
        model.addConstr(WORKERS[j].hdd_usage + Task.hdd_usage * x[j] <= WORKERS[j].max_hdd_usage)
        model.addConstr(WORKERS[j].mem_usage + Task.mem_usage * x[j] <= WORKERS[j].max_mem_usage)
        # b_hdd
        model.addConstr(WORKERS[j].hdd_usage + Task.hdd_usage * x[j] <= b_hdd[0])
        # b_mem
        model.addConstr(WORKERS[j].mem_usage + Task.mem_usage * x[j] <= b_mem[0])
    end_modeling = time.perf_counter_ns()
    modeling_time = end_modeling - start_modeling
    


    # Optimize modeal
    start_solving = time.perf_counter_ns()
    model.optimize()
    end_solving = time.perf_counter_ns()
    solver_time = end_solving - start_solving

    if model.status == GRB.OPTIMAL:
        # print("algor1_CPU time:", end_time - start_time)
        # 获取所有 x[worker.id].X 的值
        x_values = [x[j].X for j in range(NUM_WORKERS)]
    return x_values, (solver_time, modeling_time)
    





