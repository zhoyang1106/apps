import random
import time
from datetime import datetime
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from collections import deque
import matplotlib.pyplot as plt
from scipy.stats import wasserstein_distance  # 用于计算 Wasserstein 距离



TIME_SLOT_LENGTH = 1  # ms
SIMULATION_ITER_MAX = 1000 # max number of time slot
TASK_GENERATE_RATE = 30 # one task for three time slots
# MEM_CAPACITY = 500*1_000_000 # 500 MB (use any number)
# HDD_CAPACITY = 1.5*1_000_000_000 # 1.5GB (use any number)
MEM_CAPACITY = 900*1_000_000 # 500 MB (use any number)       ## 서버의 실제값
HDD_CAPACITY = 14.5*1_000_000_000 # 1.5GB (use any number)   ## 서버의 실제값
num_tasks_generated = 0
MAX_NUM_TASKS_TO_GENERATE = 3




###### DQN 전체 Start
class DQN(nn.Module):
    def __init__(self, input_dim, output_dim):
         super(DQN, self).__init__()
         self.fc1 = nn.Linear(input_dim, 512)
         self.bn1 = nn.BatchNorm1d(512)  # 批标准化
         self.fc2 = nn.Linear(512, 512)
         self.bn2 = nn.BatchNorm1d(512)
         self.fc3 = nn.Linear(512, output_dim)

    def forward(self, x):
        x = torch.relu(self.fc1(x))
        x = torch.relu(self.fc2(x))
        return self.fc3(x)


# 超参数设置
gamma = 0.99  # 折扣因子
epsilon_start = 0.9  # 探索率起始值
epsilon_end = 0.1  # 探索率结束值
epsilon_decay = 0.995  # 探索率衰减
batch_size = 128  # 经验回放批次大小
memory_size = 100000  # 经验回放存储大小
target_update = 1  # 每 5 轮更新目标网络
learning_rate = 0.001  # 学习率

# 全局变量初始化
observed_distributions = deque(maxlen=100)  # 保存任务分布历史数据
distribution_shift_threshold = 0.05  # 分布变化的阈值
epsilon = epsilon_start  # 初始化 epsilon

# 初始化 DQN 网络和目标网络
input_dim = 6  # 输入状态维度 (硬盘使用和内存使用)
output_dim = 3  # 输出维度为 3 (3 个服务器对应的动作)
policy_net = DQN(input_dim, output_dim)
target_net = DQN(input_dim, output_dim)
target_net.load_state_dict(policy_net.state_dict())  # 同步两个网络参数
target_net.eval()

# 优化器
# optimizer = optim.RAdam(policy_net.parameters(), lr=learning_rate)
optimizer = optim.AdamW(policy_net.parameters(), lr=learning_rate, weight_decay=1e-2)
# optimizer = optim.Adam(policy_net.parameters(), lr=learning_rate)
# scheduler = optim.lr_scheduler.StepLR(optimizer, step_size=500, gamma=0.9)
scheduler = optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode='min', factor=0.5, patience=10, verbose=True)



# 经验回放存储
memory = deque(maxlen=memory_size)

# 记录损失和 Q 值
losses = []
q_values_list = []

# 存储经验到 replay buffer
def store_transition(state, action, reward, next_state, done):
    memory.append((state, action, reward, next_state, done))

# 从经验回放中采样
def sample_memory(batch_size):
    return random.sample(memory, batch_size)


# 动态奖励调整机制
def adjust_reward_with_distribution_shift(reward, distribution_shift):
    # 如果分布偏移较大，增加惩罚；否则增加奖励
    clipped_shift = min(distribution_shift, 1.0)  # 设置上限为 1.0
    adjusted_reward = reward - clipped_shift * 0.1 
    if adjusted_reward < 0:
        print("Warning: Adjusted reward is negative.")
    return max(adjusted_reward, 0)
 

# 选择动作（epsilon-greedy 策略）
def choose_action(state, epsilon_start, WORKERS):
    if random.uniform(0, 1) < epsilon_start:
        # if (len(WORKERS)) == 1:
        #     return 0
        # elif (len(WORKERS)) == 2:
        #     return random.choice([0, 1])
        # else:
        #     return random.choice([0, 1, 2])
        
        # 只选择资源充足的服务器
        available_actions = [i for i, worker in enumerate(WORKERS) if worker.hdd_usage < worker.max_hdd_usage and worker.mem_usage < worker.max_mem_usage]
        if available_actions:
            return random.choice(available_actions)
        else:
            return random.choice(range(len(WORKERS)))  # 如果所有服务器都超载，则随机选择
        
    else:
        with torch.no_grad():
            state_tensor = torch.FloatTensor(state).unsqueeze(0)
            q_values = policy_net(state_tensor)
            q_values_list.append(q_values.mean().item())  # 记录 Q 值

            # 只选择资源充足的服务器
            sorted_actions = q_values.argsort(descending=True).squeeze().tolist()
            for action in sorted_actions:
                if WORKERS[action].hdd_usage < WORKERS[action].max_hdd_usage and WORKERS[action].mem_usage < WORKERS[action].max_mem_usage:
                    return action
            return sorted_actions[0]  # 如果所有服务器超载，选择 Q 值最高的

            # if (len(WORKERS)) == 1:
            #     return 0
            # elif (len(WORKERS)) == 2:
            #     if q_values.argmax().item() == 0:
            #         return 0
            #     elif q_values.argmax().item() == 1:
            #         return 1
            #     else:
            #         return random.choice([0, 1])
            # else:
            #     return q_values.argmax().item()
        

# 更新 Double DQN 网络
def update_network():
    if len(memory) < batch_size:
        return

    batch = sample_memory(batch_size)
    states, actions, rewards, next_states, dones = zip(*batch)

    states = torch.FloatTensor(states)
    actions = torch.LongTensor(actions)
    rewards = torch.FloatTensor(rewards)
    next_states = torch.FloatTensor(next_states)
    dones = torch.FloatTensor(dones)

    # 当前 Q 网络的 Q 值
    q_values = policy_net(states).gather(1, actions.unsqueeze(1)).squeeze(1)

    # Double DQN 目标 Q 值计算
    with torch.no_grad():
        next_actions = policy_net(next_states).argmax(1)
        next_q_values = target_net(next_states).gather(1, next_actions.unsqueeze(1)).squeeze(1)
        target_q_values = rewards + (1 - dones) * gamma * next_q_values

    # 计算损失
    loss = nn.MSELoss()(q_values, target_q_values)
    losses.append(loss.item())  # 记录损失

    # 反向传播和优化
    optimizer.zero_grad()
    loss.backward()
    optimizer.step()


# 更新目标网络
def update_target_network():
    target_net.load_state_dict(policy_net.state_dict())

# 获取状态
def get_state(Task, WORKERS):
    state = []
    for worker in WORKERS:
        hdd_usage = (worker.hdd_usage + Task.hdd_usage) / worker.max_hdd_usage
        mem_usage = (worker.mem_usage + Task.mem_usage) / worker.max_mem_usage
        if hdd_usage <= worker.max_hdd_usage and mem_usage <= worker.max_mem_usage:
            state.append(hdd_usage)
            state.append(mem_usage)

    # 确保状态不是空的
    if len(state) == 0:
        # print("错误: 状态为空。请检查任务和工作者的属性。")
        pass
    
    # 填充状态，确保它有 6 个元素
    
    while len(state) < 6:
        state.append(0.0)

    return np.array(state, dtype=float)





# def get_reward(response_time, action, distribution_shift):
#     # 如果 response_time 是一个浮点数，直接使用它计算奖励
#     server_response_time = response_time  # 不再需要索引，直接使用 response_time 作为浮点数
#     # reward_for_time = np.log(1.0 + 1.0 / (server_response_time + 1e-5))
#     # # 原始奖励
#     # reward = reward_for_time 

#     normalized_response_time = server_response_time / (np.max(server_response_time) + 1e-5)
#     reward_for_time = 1 / (normalized_response_time + 1e-3)  # 使用平滑项
#     adjusted_reward = reward_for_time - distribution_shift * 0.01
#     return max(adjusted_reward, 0)



def get_reward(response_time, action, Task, WORKERS, distribution_shift):
    ## 计算奖励：最小化响应时间，同时最小化 Memory 和 HDD 使用量

    # 获取选定服务器的资源占用情况
    mem_usage = WORKERS[action].mem_usage + Task.mem_usage
    hdd_usage = WORKERS[action].hdd_usage + Task.hdd_usage

    # 归一化响应时间 (目标：最小化)
    normalized_response_time = response_time / (np.max(response_time) + 1e-5)
    reward_time = 1 / (normalized_response_time + 1e-3)

    # 归一化资源使用率 (目标：最小化)
    normalized_mem_usage = mem_usage / WORKERS[action].max_mem_usage
    normalized_hdd_usage = hdd_usage / WORKERS[action].max_hdd_usage

    reward_mem = 1 / (normalized_mem_usage + 1e-3)
    reward_hdd = 1 / (normalized_hdd_usage + 1e-3)

    # 组合奖励 (加权求和)
    alpha = 1/3  # 响应时间权重
    beta = 1/3   # Memory 使用率权重
    gamma = 1/3  # HDD 使用率权重

    reward = alpha * reward_time + beta * reward_mem + gamma * reward_hdd
    adjusted_reward = reward - distribution_shift * 0.01  # 受任务分布变化影响

    return max(adjusted_reward, 0)





# 检测分布偏移
def detect_distribution_shift(observed_data, model_predictions):
    """
    使用 Wasserstein 距离检测任务分布变化。
    :param observed_data: 实际观察的任务分布
    :param model_predictions: 模型预测的输出分布
    :return: Wasserstein 距离
    """
    observed_dist = np.array(observed_data)
    predicted_dist = np.array(model_predictions)
    distance = wasserstein_distance(observed_dist, predicted_dist)
    return distance




# 动态调整 epsilon
def adjust_epsilon(distribution_shift, epsilon, min_epsilon=0.1, max_epsilon=0.9):
     base_decay = 0.995
     if distribution_shift > distribution_shift_threshold:
        return min(max_epsilon, epsilon + 0.1)
     return max(min_epsilon, epsilon * base_decay)





# 修改 DQN_Model，添加分布变化检测和动态探索调整
def DQN_Model(response_time, Task, WORKERS):
    global epsilon  # 确保 epsilon 的动态调整
    start_time = time.perf_counter_ns()
    state = get_state(Task, WORKERS)
    # print("Current state:", state)  # 打印当前状态
    action = choose_action(state, epsilon, WORKERS)  # 动态调整后的 epsilon
    end_time = time.perf_counter_ns()
    # print("dqn_CPU time:", end_time - start_time)

    observed_distributions.append(response_time)
    with torch.no_grad():
        model_predictions = policy_net(torch.FloatTensor(state).unsqueeze(0)).numpy().flatten()

    if len(observed_distributions) > 1:
        distribution_shift = detect_distribution_shift(observed_distributions[-2], model_predictions)
        epsilon_decay = adjust_epsilon(distribution_shift, epsilon, min_epsilon=0.1, max_epsilon=0.9)  # 假设任务成功率为 0.9
        epsilon = max(epsilon_end, epsilon * epsilon_decay)
    else:
        distribution_shift = 0

    reward = get_reward(response_time[action], action, Task, WORKERS, distribution_shift)
             

    next_state = get_state(Task, WORKERS)
    done = False 
    store_transition(state, action, reward, next_state, done)

    update_network()

    # 返回任务分配结果
    load_distribution = [0, 0, 0]
    load_distribution[action] = 1  # 确保只在一个服务器上分配任务

    return [action, end_time - start_time], reward  # 返回 action










