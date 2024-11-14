import random
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from collections import deque
import matplotlib.pyplot as plt



TIME_SLOT_LENGTH = 1  # ms
SIMULATION_ITER_MAX = 1000 # max number of time slot
TASK_GENERATE_RATE = 30 # one task for three time slots
# MEM_CAPACITY = 500*1_000_000 # 500 MB (use any number)
# HDD_CAPACITY = 1.5*1_000_000_000 # 1.5GB (use any number)
MEM_CAPACITY = 900*1_000_000 # 500 MB (use any number)       ## 서버의 실제값
HDD_CAPACITY = 14.5*1_000_000_000 # 1.5GB (use any number)   ## 서버의 실제값
num_tasks_generated = 0
MAX_NUM_TASKS_TO_GENERATE = 3

# initialize
NUM_WORKERS = 3
task_queues_in_processing = [[] for w in range(NUM_WORKERS)] # tasks that are being processed
tasks_done_processing = []  # tasks that are done processing





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
epsilon_start = 0.8  # 探索率起始值
epsilon_end = 0.1  # 探索率结束值
epsilon_decay = 0.990  # 探索率衰减
batch_size = 256  # 经验回放批次大小
memory_size = 50000  # 经验回放存储大小
target_update = 5  # 每 5 轮更新目标网络
learning_rate = 0.001  # 学习率

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

# 选择动作（epsilon-greedy 策略）
def choose_action(state, epsilon_start):
    if random.uniform(0, 1) < epsilon_start:
        print("No q_values: ", random.choice([0, 1, 2]))
        return random.choice([0, 1, 2])
    else:
        with torch.no_grad():
            state_tensor = torch.FloatTensor(state).unsqueeze(0)
            print("State tensor shape:", state_tensor.shape)  # 输出张量的形状
            q_values = policy_net(state_tensor)
            print("q_values: ", q_values)
            q_values_list.append(q_values.mean().item())  # 记录 Q 值
            return q_values.argmax().item()
        

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
        hdd_usage = worker.hdd_usage + Task.hdd_usage
        mem_usage = worker.mem_usage + Task.mem_usage
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



def get_reward(response_time, action):
    # 如果 response_time 是一个浮点数，直接使用它计算奖励
    server_response_time = response_time  # 不再需要索引，直接使用 response_time 作为浮点数

    # # 使用反比或归一化的方法，将响应时间转换为正向奖励
    # reward_for_time = 1.0 / (server_response_time + 1e-5)  # 避免除以 0 的情况

    # # 给动作增加奖励，假设动作编号越小效率越高
    # efficiency_bonus = 0.05 * (2 - action)

    reward_for_time = np.log(1.0 + 1.0 / (server_response_time + 1e-5))

    # 返回正向奖励
    reward = reward_for_time  # + efficiency_bonus
    return reward

    




# DQN 模型
def DQN_Model(response_time, Task, epsilon_start, WORKERS):
    state = get_state(Task, WORKERS)
    print("Current state:", state)  # 打印当前状态
    action = choose_action(state, epsilon_start)  # 确保 action 在此处定义
    reward = get_reward(response_time[action], action)

    next_state = get_state(Task, WORKERS)
    done = False  # 你可以根据任务条件设置 `done`，用于标记任务完成
    store_transition(state, action, reward, next_state, done)

    update_network()

    load_distribution = [0, 0, 0]
    load_distribution[action] = 1  # 确保只在一个服务器上分配任务

    return action  # 返回 action
###### DQN 전체 End
