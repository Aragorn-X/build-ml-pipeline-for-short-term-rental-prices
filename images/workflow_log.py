
import csv
import sys
import argparse
import pandas as pd
import yaml

config = sys.argv[1]
task = sys.argv[2]
#config = 'logs/config.yml'
#task='ships'
""" 
parser = argparse.ArgumentParser()
parser.add_argument("-c", "--config", type=str)
parser.add_argument("-t", "--task", type=str)
args = parser.parse_args()
"""
with open(config, "r") as f:
    config = yaml.safe_load(f)

path2geo = config[task]['geodbFile']
path2pr = config[task]['prFile']
path2log = config[task]['logFile']

def delete_log(log_file):
    '''
    Deleting log file content except header
    :param log_file: input log file (.csv format)
    :return:
    '''
    df = pd.read_csv(log_file, header=None)
    df.head(1).to_csv(log_file, index=False, header=False)


#Removing temporary log files
delete_log(path2geo)
delete_log(path2pr)
delete_log(path2log)

def read_csv_log(csv_file, info_data):
    with open(csv_file) as f:
        dbf = csv.reader(f)
        for i, row in enumerate(dbf):
            if i==0:
                header = row
            else:
                info_data.append(row)
    if len(info_data) > 2:    # dealing with 2 indicators
        info_data = info_data[2:]

    info_data_lst = []
    for i in range(len(info_data)):
        info_data_lst.append(lst2str(info_data[i]))

    return header, info_data_lst


def lst2str(lst):
    str_info = ','.join(str(x) for x in lst)
    return str_info



proc_info = [['NaN', 'NaN', 'NaN', 'NaN', 'NaN', 'NaN', 'NaN'],
             ['NaN', 'NaN', 'NaN', 'NaN', 'NaN', 'NaN', 'NaN']]
h_db, proc_info = read_csv_log(path2geo, proc_info)

print(proc_info)

pr_info = [['NaN', 'NaN'],
           ['NaN', 'NaN']]
h_pr, pr_info = read_csv_log(path2pr, pr_info)
print(pr_info)


log_str = []
for i in range(len(proc_info)):
    log_str.append(proc_info[i] + ',' + pr_info[0])  # pr_info[0] because the 2 indicators have the same PR

print(log_str)

#input('wait')

with open(path2log, 'a', newline='') as fs:
    log = csv.writer(fs, delimiter=',', escapechar=' ', quoting=csv.QUOTE_NONE)
    for i in range(len(log_str)):
        log.writerow([log_str[i]])
    fs.close()

