import logging
import os

from scipy.optimize import minimize
from scipy.optimize import fsolve
import math
import configparser
import numpy as np
import logging as log

root = os.path.split(os.path.realpath(__file__))[0]+"/temp"
# root = os.path.split(os.path.realpath(__file__))[0]
log.basicConfig(level=logging.INFO, filename=root+"/algorithm.log")

"""
Minimize The Maximum Latency

@parameter:
    parameter[0]: number of operators
    parameter[1]: number of tasks of each operator. [N1, N2,..,Nn]
    parameter[2]: total size of memory

--------------------- per task (2D) -----------------------
*data format: [[,,,], [,,,], ..., [,,,]]
@q_time: the queueing time of each task.

--------------------- per state instance (3D) -----------------------
*data format: [[[,,,],...[]]], [[,,,],...[]], ..., [[,,,],...[]]]
@ks: the k of each state instance of each task
@bs: the b of each state instance of each task
@state_sizes: the average state size of each state instance of each task
@item_frequencies: the item frequency [,,,] of each state instance.
"""

global N, task_num, task_instances, M
global ratio
BASE_M = 50


# calculate the TC
def cal_Tc(frequencies, item_capacity):
    C = item_capacity

    def che_approximation(Tc):
        sum = 0
        for i in frequencies:
            sum += 1 - math.exp(-1 * i * Tc)
        return sum - C

    TC = fsolve(che_approximation, np.zeros(1, dtype=np.int16))
    return TC[0]


# calculate the hit ratio
def cal_hit_ratio(frequencies, total_count, Tc):
    sum = 0.0
    for i in frequencies:
        sum += i * 1.0 / total_count * (1 - math.exp(-1 * i * Tc))
    return sum


def object_function(t, k, backlog, alpha, beta, item_frequencies, state_sizes):

    def fun(x):
        sum = 0.0
        x_index = 0
        for i in range(0, N):
            Ni = task_num[i]
            latencies = []
            for j in range(0, Ni):
                total_count = np.sum(item_frequencies[i][j])
                Tc = cal_Tc(item_frequencies[i][j], x[x_index] * ratio/state_sizes[i])
                x_index += 1

                state_access = (1 - cal_hit_ratio(item_frequencies[i][j], total_count, Tc)) * alpha[i] + beta[i]

                latency = (backlog[i][j] + 1) * (t[i][j] + state_access * k[i])
                latencies.append(latency)
            sum += np.max(latencies)
        # print(x)
        # print("ene to end latency: " + str(sum))
        return sum

    fx = lambda x : fun(x)
    return fx


def constraint():
    num_TM = np.max(task_instances) + 1
    conditions = []
    for i in range(num_TM):
        con = []
        for j in range(len(task_instances)):
            if task_instances[j] == i:
                con.append(j)
        conditions.append(con)
    cons=[]
    # for i in range(num_TM):
    # conditions=[[0], [1]]
    def constrain(x, indexes):
        sum = 0
        for index in indexes:
            sum += x[index]
        return sum - M
    if num_TM == 1:
        cons.append({'type': 'eq', 'fun': lambda x: constrain(x, conditions[0])})
    elif num_TM == 2:
        cons.append({'type': 'eq', 'fun': lambda x: constrain(x, conditions[0])})
        cons.append({'type': 'eq', 'fun': lambda x: constrain(x, conditions[1])})
    # cons = ({'type': 'eq', 'fun': lambda x: x[0] - M}, {'type': 'eq', 'fun': lambda x: x[1] - M})
    return tuple(cons)


# def get_state_size_per_task(item_frequencies, state_sizes):
#     state_size_task = []
#     for i in range(0, N):
#
#         # per operator
#         Nj = task_num[i]
#         Nh = state_num[i]
#         state_size_task.append([])
#         for j in range(0, Nj):
#
#             # per task
#             total_frequency = 0
#             avg_size = 0.0
#             for h in range(0, Nh):
#                 total_frequency += np.sum(item_frequencies[i][j][h])
#             for h in range(0, Nh):
#                 avg_size += np.sum(item_frequencies[i][j][h]) * 1.0 / total_frequency * state_sizes[i][j][h]
#             state_size_task[i].append(avg_size)
#
#     return state_size_task


# log information
def metrix_to_2Dstring(metrix):
    string = ""
    for i in range(0, N):
        string += "  @operator " + str(i) + ": "
        for j in range(0, task_num[i]):
            string += str(metrix[i][j]) + ", "
        string += "\n"
    return string

# log information
def metrix_to_1Dstring(metrix):
    string = ""
    for i in range(0, N):
        string += "  @operator " + str(i) + ": " + str(metrix[i])
        string += "\n"
    return string


def list_to_matrix(list):
    result, index_x = [], 0
    for i in range(0, N):
        result.append([])
        for j in range(0, task_num[i]):
            result[i].append(list[index_x])
            index_x += 1
    return result


def get_item_capacity(memory_size, state_size):
    result = []
    for i in range(0, N):
        result.append([])
        for j in range(0, task_num[i]):
            result[i].append(memory_size[i][j]/state_size[i])
    return result


def get_hit_ratio(capacity, frequency):
    result = []
    for i in range(0, N):
        result.append([])
        for j in range(0, task_num[i]):
            Tc = cal_Tc(frequency[i][j], capacity[i][j])
            total_count = np.sum(frequency[i][j])
            hit_ratio = cal_hit_ratio(frequency[i][j], total_count, Tc)
            result[i].append(hit_ratio)
    return result


def get_latency(t, k, backlog, alpha, beta, hit_ratios):
    result = []
    for i in range(0, N):
        result.append([])
        for j in range(0, task_num[i]):
            access_time = alpha[i] * (1 - hit_ratios[i][j]) + beta[i]
            latency = (backlog[i][j] + 1) * (t[i][j] + access_time * k[i])
            result[i].append(latency)
    return result


def get_max_latency(latency):
    result = []
    for i in range(0, N):
        result.append(np.max(latency[i]))
    return result


def print_basic_info():
    parameter_info = "\n  -------------------------Basic Information-------------------------\n"
    parameter_info += "  There are total " + str(N) + " stateful operators\n"
    parameter_info += "  There are " + str(task_num) + " number of tasks for each operator in order\n"
    parameter_info += "  Total " + str(M) + " MB memory"
    log.info(parameter_info)


def print_result(m, state_size, frequencies, t, k, backlog, alpha, beta, end_to_end_latency):
    m_matrix = list_to_matrix(m)
    parameter_info = "\n  -------------------------Result to Check-------------------------\n"

    parameter_info += "  optimal memory size:\n"
    parameter_info += metrix_to_2Dstring(m_matrix) + "\n"

    parameter_info += "  average state size:\n"
    parameter_info += metrix_to_1Dstring(state_size) + "\n"

    parameter_info += "  item capacity:\n"
    item_capacity = get_item_capacity(m_matrix, state_size)
    parameter_info += metrix_to_2Dstring(item_capacity) + "\n"

    parameter_info += "  hit ratio:\n"
    hit_ratio = get_hit_ratio(item_capacity, frequencies)
    parameter_info += metrix_to_2Dstring(hit_ratio) + "\n"

    parameter_info += "  alpha:\n"
    parameter_info += metrix_to_1Dstring(alpha) + "\n"

    parameter_info += "  beta:\n"
    parameter_info += metrix_to_1Dstring(beta) + "\n"

    parameter_info += "  front end service:\n"
    parameter_info += metrix_to_2Dstring(t) + "\n"

    parameter_info += "  number of state access per tuple:\n"
    parameter_info += metrix_to_1Dstring(k) + "\n"

    parameter_info += "  backlog:\n"
    parameter_info += metrix_to_2Dstring(backlog) + "\n"

    parameter_info += "  latency per task:\n"
    latency = get_latency(t, k, backlog, alpha, beta, hit_ratio)
    parameter_info += metrix_to_2Dstring(latency) + "\n"

    parameter_info += "  maximum latency per operator:\n"
    max_latency = get_max_latency(latency)
    parameter_info += "  " + str(max_latency) +"\n"+"\n"

    parameter_info += "  maximum end to end latency:\n"
    parameter_info += "  through hand:  " + str(np.sum(max_latency)) +"\n"
    parameter_info += "  through scipy: " + str(end_to_end_latency)

    log.info(parameter_info)


def init_x0(length, value):
    array = []
    for i in range(length):
        array.append(int(value / length))
    return array


def min_max_latency_main(parameters, t, k, backlog, alpha, beta, state_sizes, item_frequencies):
    global N, task_num, task_instances, M, ratio
    N = parameters[0]
    task_num = parameters[1]
    task_instances = parameters[2]
    M = int(parameters[3])
    M = BASE_M

    print_basic_info()

    # state_sizes_task = get_state_size_per_task(item_frequencies, state_sizes)

    fun = object_function(t, k, backlog, alpha, beta, item_frequencies, state_sizes)
    con = constraint()

    b, bnds = (0, M), []
    for i in range(0, np.sum(task_num)):
        bnds.append(b)

    x0_array = init_x0(np.sum(task_num), M)
    x0 = np.array(x0_array)

    res2 = minimize(fun, x0, method='SLSQP', bounds=tuple(bnds), constraints=con)

    log.info("Optimization problem :\t{}".format(res2.message))  #
    print("Optimization problem :\t{}".format(res2.message))  #


    results = [i*ratio for i in res2.x]

    print_result(results, state_sizes, item_frequencies, t, k, backlog, alpha, beta, res2.fun)
    return results
    # print("xOpt = {}".format(res2.x))  #
    # print("min f(x) = {:.4f}".format(res2.fun))  #


# load messages and write results.
"""
use | to separate operator
use ; to separate task
use , to separate state instance
use - to separate frequency
"""

def parse_operator_int(raw_data):
    array_str, int_list = raw_data.split("|"), []
    for item in array_str:
        int_list.append(int(item))
    return int_list


def parse_operator_float(raw_data):
    array_str, int_list = raw_data.split("|"), []
    for item in array_str:
        int_list.append(float(item))
    return int_list


def parse_task(raw_data):
    result_list = []
    for task_str in raw_data.split("|"):
        task_list = []
        for item in task_str.split(";"):
            task_list.append(float(item))
        result_list.append(task_list)
    return result_list


def parse_task_int(raw_data):
    result_list = []
    for task_str in raw_data.split("|"):
        for item in task_str.split(";"):
            result_list.append(int(item))
    return result_list


def parse_state(raw_data):
    result_list = []
    for task_str in raw_data.split("|"):
        task_list = []
        for state_str in task_str.split(";"):
            state_list = []
            for item in state_str.split(","):
                state_list.append(float(item))
            task_list.append(state_list)
        result_list.append(task_list)
    return result_list


def parse_item_frequency(raw_data):
    result_list = []
    for task_str in raw_data.split("|"):
        task_list = []
        for items in task_str.split(";"):
            item_list = []
            for item in items.split("-"):
                item_list.append(int(item))
            task_list.append(item_list)
        result_list.append(task_list)
    return result_list


def load_basic_info():
    file, parameters = root+"/basic_info.properties", []
    config = configparser.ConfigParser()
    config.read(file)
    parameters.append(config.getint("info", "operator.num"))
    parameters.append(parse_operator_int(config.get("info", "task.num")))
    parameters.append(parse_task_int(config.get("info", "task.instance")))
    parameters.append(config.getint("info", "memory.size"))
    global ratio
    ratio = parameters[3] * 1.0 / BASE_M
    return parameters


def preprocess(raw):
    state_sizes = []
    for i in raw:
        array = []
        for j in i:
            array.append(j / ratio)
        state_sizes.append(array)
    return state_sizes


def load_metrics():
    file = root+"/metrics.data"
    config = configparser.ConfigParser()
    config.read(file)
    t = parse_task(config.get("data", "frontEndTime"))
    k = parse_operator_float(config.get("data", "k"))
    backlog = parse_task(config.get("data", "backlog"))
    alpha = parse_operator_float(config.get("data", "alpha"))
    beta = parse_operator_float(config.get("data", "beta"))
    state_sizes = parse_operator_float(config.get("data", "state.size"))
    # state_sizes = preprocess(state_sizes_raw)
    item_frequency = parse_item_frequency(config.get("data", "item.frequency"))
    epoch = config.getint("data", "epoch")
    return t, k, backlog, alpha, beta, state_sizes, item_frequency, epoch


def write_result(epoch, results):
    file = root+"/result.data"
    f1 = open(file, 'w')
    f1.write("epoch="+str(epoch)+"\n")
    f1.write("results="+str(results))
    f1.close()


if __name__ == '__main__':
    parameters = load_basic_info()
    t, k, backlog, alpha, beta, state_sizes, item_frequencies, epoch = load_metrics()
    results = min_max_latency_main(parameters, t, k, backlog, alpha, beta, state_sizes, item_frequencies)
    write_result(epoch, results)
