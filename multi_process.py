import os, subprocess
import multiprocessing as mp
import time
import sys

data_files_output = subprocess.check_output(["ls",sys.argv[1])
data_files_list = []

#Split the input into batches of 8. This is to run eight processes in parallel
if (len(data_files_output.split()))>=8:
    for data_files_list_index in range(0,len(data_files_output.split())):
        if data_files_list_index % 8 == 0:
            data_files_list.append(data_files_output.split()[data_files_list_index-8:data_files_list_index])
    data_files_list.append(data_files_output.split()[data_files_list_index-1:])
    data_files_list = data_files_list[1:]
else:
    data_files_list = data_files_output.split()


mp_output = mp.Queue()

#This could launch the final_master py file
def multi_process_init(i,o):
    i = sys.argv[1]+"/"+i
    subprocess.call(["python2.7","master_final.py",i])
    o.put(i)

#Should be a for loop to execute across every element of data_files_list
processes = [mp.Process(target=multi_process_init, args=(data_files_item,mp_output)) for data_files_item in data_files_list[0][:4]]

for p in processes:
    p.start()

for p in processes:
    p.join()

results = [mp_output.get() for p in processes]

print(results)

