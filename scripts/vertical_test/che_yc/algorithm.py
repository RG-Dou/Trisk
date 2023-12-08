import logging
import os
import sys

from scipy.optimize import fsolve
import math
import configparser
import numpy as np
import logging as log

coldMissFrq = 0.0


# calculate the TC
def cal_Tc(frequencies, item_capacity):
    C = item_capacity

    def che_approximation(Tc):

        sum = 0
        # for i in frequencies:
        #     sum += 1 - math.exp(-1 * i * Tc)
        for i in range (0, len(frequencies) - 1, 2):
            sum += (1 - math.exp(-1 * frequencies[i] * Tc)) * frequencies[i+1]
        return sum * (1 - coldMissFrq) - C

    TC = fsolve(che_approximation, np.zeros(1, dtype=np.int16))
    return TC[0]


# calculate the hit ratio
def cal_hit_ratio(frequencies, total_count, Tc):
    sum = 0.0
    # for i in frequencies:
    #     sum += i * 1.0 / total_count * (1 - math.exp(-1 * i * Tc))
    for i in range (0, len(frequencies) - 1, 2):
        frequency = frequencies[i]
        counter = frequencies[i+1]
        sum += (frequency * 1.0 / total_count * (1 - math.exp(-1 * frequency * Tc))) * counter
    return sum * (1 - coldMissFrq)


def get_hit_ratio(capacity, frequency, total_items):

    Tc = cal_Tc(frequency, capacity)
    total_count = total_items
    hit_ratio = cal_hit_ratio(frequency, total_count, Tc)
    return hit_ratio


def parse_item_frequency(raw_data):
    item_list, total  = [], 0
    for item in raw_data.split("-"):
        item_list.append(int(item))

    for i in range (0, len(item_list) - 1, 2):
        total += item_list[i] * item_list[i+1]

    return item_list, total


def load_metrics(file):
    config = configparser.ConfigParser()
    config.read(file)
    capacity = int(config.get("data", "capacity"))
    item_frequency, total_item = parse_item_frequency(config.get("data", "item.frequency"))
    global coldMissFrq
    coldMissFrq = float(config.get("data", "cold.miss"))
    return capacity, item_frequency, total_item,


if __name__ == '__main__':
    arguments = sys.argv
    capacity, frequencies, total_item = load_metrics(arguments[1])
    hit_ratio = get_hit_ratio(capacity, frequencies, total_item)
    print(hit_ratio)

