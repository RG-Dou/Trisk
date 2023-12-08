import configparser
import numpy as np
import re
import os
import pdb

import fitting
import util as util

cache_size = np.arange(10, 60, 40)


nexmark_path = ['bash', 'nexmark-run.sh']
# 1. app; 2. controller; 3. group into one slot or not; 4. bid rate; 5. number of try; 6. skewness; 7. total memory size; 8. Cache Replace Policy; 9. algorithm (CacheMissEqn or Che)
nexmark_args = ['q20', 'TestInitMemoryManager', 'true', '400', '1', '1.0', '100', 'LRU', 'CacheMissEqn']
pattern = re.compile(r'metadata path: (.+)')

algorithm_path = ['python3', 'algorithm.py']
cacheMissEqn_exe = ['../../../Trisk-on-Flink/tools/algorithms/cacheMissEqn/fit']

def read_meta_data(file, section, key):
    config = configparser.ConfigParser()
    config.read(file)
    return config.get(section, key)


def write_meta_data(file):
    capacity = cache * 1.0 / float(read_meta_data(file, "data", "state.size"))
    item_capacity = read_meta_data(file, "data", "item.frequency")

    config = configparser.ConfigParser()
    config.add_section("data")
    config.set("data", "capacity", str(int(capacity)))
    config.set("data", "item.frequency", item_capacity)
    config.set("data", "cold.miss", "0.0")

    with open(file + ".copy", 'w') as configfile:
        config.write(configfile)


def get_true_hit_ratio(path):
    log_file = path + "/flink-drg-standalonesession-0-dragon-sane.out"
    with open(log_file, 'r') as file:
        lines = file.readlines()

    last_line = lines[-1].strip()
    key_value_pairs = last_line.split(', ')
    data_dict = {}
    for pair in key_value_pairs:
        key, value = pair.split(':')
        data_dict[key.strip()] = value

    return data_dict['hitRatio']



if __name__ == '__main__':
    xs, ys = {}, {}
    keys = ["True", "Che", "CacheMissEqn"]
    for key in keys:
        xs[key], ys[key] = [], []

    root_path = ""

    for cache in cache_size:
        nexmark_args[6] = str(cache)
        response = util.run_shell(nexmark_path, nexmark_args)

        meta_path = pattern.findall(response)[0]
        root_path = os.path.dirname(meta_path)
        metrics_data = meta_path+"/metrics.data"

        write_meta_data(metrics_data)

        che_hit = float(util.run_shell(algorithm_path, [metrics_data + ".copy"]))
        true_hit = float(get_true_hit_ratio(meta_path))

        xs["Che"].append(cache)
        ys["Che"].append(che_hit)
        xs["True"].append(cache)
        ys["True"].append(true_hit)

    labels = {
        "x": "Cache Size",
        "y": "Miss Ratio",
        "title": ""
    }
    root_path += "/output"
    xs["CacheMissEqn"], ys["CacheMissEqn"], labels["title"] = fitting.fitting_c(root_path, xs["True"], ys["True"])
    items = ["True", "Che", "CacheMissEqn"]
    util.draw_dot(xs, ys, labels, items, root_path)
