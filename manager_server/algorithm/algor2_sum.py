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







def Optimization_Model2(response_time, Tasks, WORKERS):
    num_tasks = len(Tasks)
    num_workers = len(WORKERS)

    # weight (sum=1, random)
    alpha = 1/3
    beta = 1/3
    gamma = 1/3

    # Maximum value measured by 20000 tests
    hdd_usage_std_max = 0.166666667
    mem_usage_std_max = 0.166666667
    max_response_times = [max(response_time[i]) for i in range(num_tasks)]


    # Create model
    start_modeling = time.perf_counter_ns()
    model = gp.Model("server_optimization")

    # Create variables
    x = model.addVars(num_tasks, num_workers, vtype=GRB.BINARY, name="x")

    # Set objective function：
    for i in range(num_tasks):
        hdd_usage_std = gp.quicksum(((Tasks[i].hdd_usage * x[i, j] + WORKERS[j].hdd_usage) - (Tasks[i].hdd_usage + WORKERS[j].hdd_usage) / num_workers) ** 2 for j in range(num_workers))
        mem_usage_std = gp.quicksum(((Tasks[i].mem_usage * x[i, j] + WORKERS[j].mem_usage) - (Tasks[i].mem_usage + WORKERS[j].mem_usage) / num_workers) ** 2 for j in range(num_workers))

        model.setObjective(alpha / hdd_usage_std_max * hdd_usage_std + beta / mem_usage_std_max * mem_usage_std + gamma / max_response_times[i] * gp.quicksum(response_time[i][j] * x[i, j] for j in range(num_workers)), GRB.MINIMIZE)

    # Add constraint
    for i in range(num_tasks):
        # 添加约束：每个任务只能分配给一个 worker
        model.addConstr(gp.quicksum(x[i, j] for j in range(num_workers)) == 1)
        for j in range(num_workers):
            # 添加约束：每个 worker 的资源不能超过限制
            model.addConstr(WORKERS[j].hdd_usage + Tasks[i].hdd_usage * x[i, j] <= WORKERS[j].max_hdd_usage)
            model.addConstr(WORKERS[j].mem_usage + Tasks[i].mem_usage * x[i, j] <= WORKERS[j].max_mem_usage)
    end_modeling = time.perf_counter_ns()
    modeling_time = end_modeling - start_modeling



    model.setParam(GRB.Param.PoolSearchMode, 2)  # 深度搜索模式，尽量生成多种解
    model.setParam(GRB.Param.PoolSolutions, 10)  # 设置最多保存 10 个不同的解
    model.setParam(GRB.Param.PoolGap, 0.2)       # 强制每个解之间有至少 20% 的差异



    # Optimize modeal
    start_solving = time.perf_counter_ns()
    model.optimize()
    end_solving = time.perf_counter_ns()
    solver_time = end_solving - start_solving

    # Print results
    # 输出结果矩阵
    # if model.status == GRB.OPTIMAL:
    #     end_time = time.perf_counter_ns()
    #     result_matrix = np.zeros((num_tasks, num_workers), dtype=int)
    #     for i in range(num_tasks):
    #         for j in range(num_workers):
    #             result_matrix[i, j] = int(x[i, j].X)
    #     print(result_matrix)
    #     chosen_workers = []
    #     chosen_workers_index = np.argmax(result_matrix, axis=1)
    #     for i in range(len(chosen_workers_index)):
    #         chosen_workers.append(WORKERS[chosen_workers_index[i]]) 
    # return chosen_workers, end_time - start_time



    # 检查解的数量
    solution_count = model.SolCount
    # 初始化变量，用于存储最均匀分布的解
    best_solution = None
    best_distribution_diff = float('inf')  # 用于记录最小的任务分布差异
    for k in range(solution_count):
        model.setParam(GRB.Param.SolutionNumber, k)
        result_matrix = np.zeros((num_tasks, num_workers), dtype=int)
        
        for i in range(num_tasks):
            for j in range(num_workers):
                result_matrix[i, j] = int(x[i, j].Xn)
        
        # 计算每个 worker 的任务数量
        task_distribution = result_matrix.sum(axis=0)
        distribution_diff = max(task_distribution) - min(task_distribution)  # 计算最大最小任务数量的差异
    
        # 更新最均匀分布的解
        if distribution_diff < best_distribution_diff:
            best_distribution_diff = distribution_diff
            best_solution = result_matrix


    # 输出最均匀分布的解
    if best_solution is not None:
        chosen_workers = []
        # print(f"Best Balanced Solution (Task Distribution: {best_task_distribution}):")
        chosen_workers_index = np.argmax(best_solution, axis=1)
        for i in range(len(chosen_workers_index)):
            chosen_workers.append(WORKERS[chosen_workers_index[i]]) 
    return chosen_workers, (solver_time, modeling_time)




