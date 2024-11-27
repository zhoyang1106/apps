import random
import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from collections import deque
import matplotlib.pyplot as plt

# 参数设置
TIME_SLOT_LENGTH = 1  # ms
SIMULATION_ITER_MAX = 1000  # 最大时间片
TASK_GENERATE_RATE = 30  # 每 3 个时间片生成一个任务
MEM_CAPACITY = 900 * 1_000_000  # 内存容量
HDD_CAPACITY = 14.5 * 1_000_000_000  # 硬盘容量





# 超参数设置
gamma = 0.99  # 折扣因子
epsilon_start = 0.7  # 初始探索率
epsilon_end = 0.1  # 最低探索率
epsilon_decay = 0.995  # 探索率衰减
batch_size = 128  # 经验回放批次大小
memory_size = 100000  # 经验回放存储大小
learning_rate = 0.001  # 学习率
target_update_interval = 3  # 目标网络更新间隔
tau = 0.01  # 软更新系数



# 优先经验回放类
class PrioritizedReplayBuffer:
    def __init__(self, capacity, alpha=0.6):
        self.capacity = capacity
        self.alpha = alpha
        self.buffer = []
        self.priorities = np.zeros((capacity,), dtype=np.float32)
        self.position = 0

    def add(self, experience, td_error):
        priority = (abs(td_error) + 1e-5) ** self.alpha  # 确保非零
        if len(self.buffer) < self.capacity:
            self.buffer.append(experience)
        else:
            self.buffer[self.position] = experience
        self.priorities[self.position] = max(priority, 1e-5)  # 确保优先级非零
        self.position = (self.position + 1) % self.capacity

    def sample(self, batch_size, beta=0.4):
        if len(self.buffer) == 0:
            raise ValueError("Replay buffer is empty, cannot sample!")

        scaled_priorities = self.priorities[:len(self.buffer)] ** beta
        probs = scaled_priorities / (scaled_priorities.sum() + 1e-5)  # 防止除以零
        if np.any(np.isnan(probs)) or not np.isfinite(probs).all():
            raise ValueError("Probabilities contain NaN or invalid values!")

        indices = np.random.choice(len(self.buffer), batch_size, p=probs)
        samples = [self.buffer[i] for i in indices]
        states, actions, rewards, next_states, dones = zip(*samples)
        weights = (1.0 / (len(self.buffer) * probs[indices])) ** (1 - beta)
        weights /= weights.max()
        return (np.array(states), np.array(actions), np.array(rewards),
                np.array(next_states), np.array(dones), weights, indices)

    def update_priorities(self, indices, td_errors):
        for i, td_error in zip(indices, td_errors):
            self.priorities[i] = max((abs(td_error) + 1e-5) ** self.alpha, 1e-5)  # 确保非零



# DQN 网络
class ResidualDQN(nn.Module):
    def __init__(self, input_dim, output_dim):
        super(ResidualDQN, self).__init__()
        self.fc1 = nn.Linear(input_dim, 512)
        self.fc2 = nn.Linear(512, 512)
        self.fc3 = nn.Linear(512, output_dim)

    def forward(self, x):
        x1 = torch.relu(self.fc1(x))
        x2 = torch.relu(self.fc2(x1))
        return self.fc3(x2 + x1)


# 初始化
input_dim = 6
output_dim = 3
policy_net = ResidualDQN(input_dim, output_dim)
target_net = ResidualDQN(input_dim, output_dim)
target_net.load_state_dict(policy_net.state_dict())
target_net.eval()
optimizer = optim.AdamW(policy_net.parameters(), lr=learning_rate, weight_decay=1e-2)
memory = PrioritizedReplayBuffer(memory_size)


# 存储损失和 Q 值
losses = []
q_values_list = []


# 存储经验
def store_transition(state, action, reward, next_state, done):
    with torch.no_grad():
        state_tensor = torch.FloatTensor(state).unsqueeze(0)
        next_state_tensor = torch.FloatTensor(next_state).unsqueeze(0) 
        q_value = policy_net(state_tensor)[0, action].item()
        next_q_value = target_net(next_state_tensor).max(1)[0].item()
        td_error = reward + gamma * next_q_value * (1 - done) - q_value
    memory.add((state, action, reward, next_state, done), td_error)

# 更新网络
def update_network():
    if len(memory.buffer) < batch_size:
        return

    states, actions, rewards, next_states, dones, weights, indices = memory.sample(batch_size)
    states = torch.FloatTensor(states)
    actions = torch.LongTensor(actions)
    rewards = torch.FloatTensor(rewards)
    next_states = torch.FloatTensor(next_states)
    dones = torch.FloatTensor(dones)
    weights = torch.FloatTensor(weights)

    q_values = policy_net(states).gather(1, actions.unsqueeze(1)).squeeze(1)
    with torch.no_grad():
        next_actions = policy_net(next_states).argmax(1)
        next_q_values = target_net(next_states).gather(1, next_actions.unsqueeze(1)).squeeze(1)
        target_q_values = rewards + (1 - dones) * gamma * next_q_values

    loss = (weights * (q_values - target_q_values) ** 2).mean()
    losses.append(loss.item())

    optimizer.zero_grad()
    loss.backward()
    optimizer.step()

    td_errors = (q_values - target_q_values).detach().cpu().numpy()
    memory.update_priorities(indices, td_errors)

# 软更新目标网络
def soft_update_target_network():
    for target_param, local_param in zip(target_net.parameters(), policy_net.parameters()):
        target_param.data.copy_(tau * local_param.data + (1.0 - tau) * target_param.data)

# 选择动作
def choose_action(state, epsilon):
    if random.uniform(0, 1) < epsilon:
        return random.choice([0, 1, 2])
    else:
        with torch.no_grad():
            state_tensor = torch.FloatTensor(state).unsqueeze(0)
            q_values = policy_net(state_tensor)
            q_values_list.append(q_values.mean().item())
            return q_values.argmax().item()

# 奖励函数
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

# 获取状态
def get_state(Task, WORKERS):
    state = []
    for worker in WORKERS:
        hdd_usage = worker.hdd_usage + Task.hdd_usage
        mem_usage = worker.mem_usage + Task.mem_usage
        state.extend([hdd_usage, mem_usage])
    while len(state) < 6:
        state.append(0.0)
    return np.array(state, dtype=float)




# DQN 模型
def DQN_Model(response_time, Task, epsilon_start, WORKERS):
    state = get_state(Task, WORKERS)
    # print("Current state:", state)  # 打印当前状态
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
