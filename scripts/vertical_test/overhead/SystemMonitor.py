import sys

import psutil
import os
import time
#
# print(psutil.cpu_percent())
# print(psutil.cpu_stats())
# print(psutil.cpu_freq())
#
# print(psutil.cpu_percent())
# # gives an object with many fields
# psutil.virtual_memory()
# # you can convert that object to a dictionary
# dict(psutil.virtual_memory()._asdict())
# # you can have the percentage of used RAM
# print(psutil.virtual_memory().percent)
# # you can calculate percentage of available memory
# psutil.virtual_memory().available * 100 / psutil.virtual_memory().total

if __name__ == '__main__':
    file_name = sys.argv[1]
    if os.path.exists(file_name):
        os.remove(file_name)
    file_object = open(file_name, 'a')
    while True:
        load1, load5, load15 = psutil.getloadavg()
        cpu_usage = (load15 / os.cpu_count()) * 100
        cpu1 = psutil.cpu_percent(1)*64/100
        cpu2 = cpu_usage * 32/100
        memory = psutil.virtual_memory()[2]*64/100
        now = time.time()
        log = "time:" + str(now) + " cpu1:" + str(cpu1) + " cpu2:" + str(cpu2) + " mem:" + str(memory) + "\n"
        file_object.write(log)