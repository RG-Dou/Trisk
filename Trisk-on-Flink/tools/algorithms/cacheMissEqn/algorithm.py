import logging
import os

from scipy.optimize import minimize
from scipy.optimize import fsolve
import math
import configparser
import numpy as np
import logging as log
from math import sqrt

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
BASE_M = 1024


# cache miss equation
def cache_miss_eqn(x, parameter):
    Pstar, P0, Mstar, M0 = parameter["Pstar"], parameter["P0"], parameter["Mstar"], parameter["M0"]
    if x < M0:
        return 1

    if x < Mstar:
        return 0.5 * (1 + (Mstar - M0) / (x - M0) + sqrt((1 + (Mstar - M0) / (x - M0)) * (1 + (Mstar - M0) / (x - M0)) - 4)) * (Pstar + P0) - P0

    return Pstar


def read_parameters_from_file(file_name):
    parameters = {
        "Mstar": None,
        "M0": None,
        "Pstar": None,
        "P0": None,
        "title": None
    }

    with open(file_name, "r") as file:
        for line in file:
            if " " not in line:
                if "Mstar" in line:
                    parameters["Mstar"] = float(line.split("=")[1].strip())
                elif "M0" in line:
                    parameters["M0"] = float(line.split("=")[1].strip())
                elif "Pstar" in line:
                    parameters["Pstar"] = float(line.split("=")[1].strip())
                elif "P0" in line:
                    parameters["P0"] = float(line.split("=")[1].strip())
            elif "set title" in line:
                parameters["title"] = line.split("\"")[1]

    return parameters


def object_function(t, k, backlog, alpha, beta, parameters):

    def fun(x):
        sum = 0.0
        x_index = 0
        for i in range(0, N):
            Ni = task_num[i]
            latencies = []
            for j in range(0, Ni):
                miss_ratio = cache_miss_eqn(x[x_index] * ratio, parameters[i][j])
                x_index += 1

                state_access = miss_ratio * alpha[i] + beta[i]

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


def get_hit_ratio(cache_size, parameters):
    result = []
    for i in range(0, N):
        result.append([])
        for j in range(0, task_num[i]):
            hit_ratio = cache_miss_eqn(cache_size[i][j], parameters[i][j])
            result[i].append(hit_ratio)
    return result


def get_latency(t, k, backlog, alpha, beta, hit_ratios):
    result = []
    for i in range(0, N):
        result.append([])
        for j in range(0, task_num[i]):
            access_time = alpha[i] * (1 - hit_ratios[i][j]) + beta[i]
            latency = (backlog[i][j] + 1) * (t[i][j] + access_time * k[i])
            # u = t[i][j] + access_time * k[i]
            # latency = 1 / (1 / u - arrival_rate[i][j])
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


def print_result(m, t, k, backlog, alpha, beta, parameters_cacheMissEqn, end_to_end_latency):
    m_matrix = list_to_matrix(m)
    parameter_info = "\n  -------------------------Result to Check-------------------------\n"

    parameter_info += "  optimal memory size:\n"
    parameter_info += metrix_to_2Dstring(m_matrix) + "\n"

    parameter_info += "  alpha:\n"
    parameter_info += metrix_to_1Dstring(alpha) + "\n"

    parameter_info += "  beta:\n"
    parameter_info += metrix_to_1Dstring(beta) + "\n"

    parameter_info += "  front end service:\n"
    parameter_info += metrix_to_2Dstring(t) + "\n"

    # //////////
    parameter_info += "  hit ratio:\n"
    hit_ratio = get_hit_ratio(m_matrix, parameters_cacheMissEqn)
    parameter_info += metrix_to_2Dstring(hit_ratio) + "\n"

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


# def init_x0(state_sizes, total_item, backlog, total_memory):
#     array, total_size, operator_sizes, task_sizes = [], 0, [], []
#     for i in range(0, N):
#         operator_size = 0
#         for j in range(0, task_num[i]):
#             size = total_item[i][j] * state_sizes[i]
#             operator_size += size
#             total_size += size
#             task_sizes.append(int(size))
#         operator_sizes.append(operator_size)
#
#     for i in range(0, N):
#         operator_sizes[i] = operator_sizes[i] * 1.0/ total_size * total_memory
#     for i in range(0, N):
#         operator_memory = operator_sizes[i]
#         total_backlog = 0
#         for j in range(0, task_num[i]):
#             total_backlog += backlog[i][j]
#         for j in range(0, task_num[i]):
#             array.append(int(backlog[i][j] * 1.0 / total_backlog * operator_memory))
#     return array, task_sizes


def min_max_latency_main(t, k, backlog, alpha, beta, parameters_cacheMissEqn):

    fun = object_function(t, k, backlog, alpha, beta, parameters_cacheMissEqn)
    con = constraint()

    x0_array = init_x0(np.sum(task_num), M)
    # x0_array, task_sizes = init_x0(state_sizes, total_item, backlog, M)
    x0 = np.array(x0_array)

    b, bnds = (0, M), []
    for i in range(0, np.sum(task_num)):
        # c = [0, task_sizes[i]]
        # bnds.append(tuple(c))
        bnds.append(b)

    res2 = minimize(fun, x0, method='SLSQP', bounds=tuple(bnds), constraints=con)

    log.info("Optimization problem :\t{}".format(res2.message))  #
    print("Optimization problem :\t{}".format(res2.message))  #


    results = [i*ratio for i in res2.x]

    print_result(results, t, k, backlog, alpha, beta, parameters_cacheMissEqn, res2.fun)
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


def load_basic_info():
    file, parameters = root+"/basic_info.properties", []
    config = configparser.ConfigParser()
    config.read(file)
    parameters.append(config.getint("info", "operator.num"))
    parameters.append(parse_operator_int(config.get("info", "task.num")))
    parameters.append(parse_task_int(config.get("info", "task.instance")))
    parameters.append(config.getint("info", "memory.size"))
    global N, task_num, task_instances, M, ratio
    ratio = parameters[3] * 1.0 / BASE_M


    N = parameters[0]
    task_num = parameters[1]
    task_instances = parameters[2]
    M = int(parameters[3])
    M = BASE_M

    print_basic_info()

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
    # state_sizes = parse_operator_float(config.get("data", "state.size"))
    # item_frequency, total_item = parse_item_frequency(config.get("data", "item.frequency"))
    # arrival_rate = parse_task(config.get("data", "arrivalRate"))
    epoch = config.getint("data", "epoch")
    return t, k, backlog, alpha, beta, epoch
    # return t, k, backlog, alpha, beta, state_sizes, item_frequency, total_item, arrival_rate, epoch


def load_cacheMissParam():
    parameters_cacheMissEqn = {}
    for i in range(N):
        parameters_cacheMissEqn[i] = {}
        for j in range(task_num[i]):
            file = root + "/points-" + str(i) + "-" + str(j) + ".g"
            parameters_cacheMissEqn[i][j] = read_parameters_from_file(file)
    return parameters_cacheMissEqn



def write_result(epoch, results):
    file = root+"/result.data"
    f1 = open(file, 'w')
    f1.write("epoch="+str(epoch)+"\n")
    f1.write("results="+str(results))
    f1.close()


if __name__ == '__main__':
    load_basic_info()
    parameters_cacheMissEqn = load_cacheMissParam()
    t, k, backlog, alpha, beta, epoch = load_metrics()
    results = min_max_latency_main(t, k, backlog, alpha, beta, parameters_cacheMissEqn)
    write_result(epoch, results)
