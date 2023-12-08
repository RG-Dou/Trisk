import sys

import numpy as np
import subprocess
import os
from math import sqrt

x, y = [], []

def my_function(x, Pstar, P0, Mstar, M0):
    if x < M0:
        return 1

    if x < Mstar:
        return 0.5 * (1 + (Mstar - M0) / (x - M0) + sqrt((1 + (Mstar - M0) / (x - M0)) * (1 + (Mstar - M0) / (x - M0)) - 4)) * (Pstar + P0) - P0

    return Pstar


def check_and_create_folder(folder_path):
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        print(f"文件夹 '{folder_path}' 创建成功！")
    else:
        print(f"文件夹 '{folder_path}' 已经存在。")


def read_parameters_from_file(filename):
    parameters = {
        "Mstar": None,
        "M0": None,
        "Pstar": None,
        "P0": None,
        "title": None
    }

    with open(filename, "r") as file:
        for line in file:
            if " " not in line:
                if "Mstar" in line:
                    parameters["Mstar"] = float(line.split("=")[1].strip())
                elif "M0" in line:
                    print(line)
                    parameters["M0"] = float(line.split("=")[1].strip())
                elif "Pstar" in line:
                    parameters["Pstar"] = float(line.split("=")[1].strip())
                elif "P0" in line:
                    parameters["P0"] = float(line.split("=")[1].strip())
            elif "set title" in line:
                parameters["title"] = line.split('"')[1]
                print(parameters["title"])

    return parameters


def get_parameters(folder_path, xs):
    parameters = read_parameters_from_file(folder_path+"/points.g")
    x_fit, y_fit = [], []
    # for x in range(50, xs[-1], 50):
    for x in range(1, xs[-1], 2):
        x_fit.append(x)
        y_fit.append(my_function(x, parameters["Pstar"], parameters["P0"], parameters["Mstar"], parameters["M0"]))
    return x_fit, y_fit, parameters["title"]


def fitting_c(folder_path, xs, ys):
    check_and_create_folder(folder_path)
    file_path = folder_path + "/points"

    with open(file_path, 'w') as file:
        for index in range(len(xs)):
            newline = str(xs[index]) + " " + str(ys[index]) + "\n"
            file.write(newline)

    execute_c_script(file_path)
    return get_parameters(folder_path, xs)


def execute_c_script(file):
    script_path = "../../../Trisk-on-Flink/tools/algorithms/cacheMissEqn/fit"
    try:
        subprocess.run([script_path, file], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error while executing the shell script: {e}")
        sys.exit(1)


