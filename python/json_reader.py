import json
import csv
import pandas as pd
import os
import re
import yaml
from joblib import Parallel, delayed
import argparse
from recurse_dict import recurse_dict, process_json

parser = argparse.ArgumentParser()
parser.add_argument('-n','--rename', help='YAML file with renaming patterns for the hierarchical fields', type=str)
parser.add_argument('-d','--depth-rename', help='YAML file with list of variables expected to be at a terminal node but should get depth renaming', type=str, default=None)
parser.add_argument('-p', '--path', help='Path to find JSON files', type=str, required=True)
parser.add_argument('-f', '--filename', help='output file name, will be a prefix when batch size is less than total number of files', type=str, required=True)
parser.add_argument('-o', '--outpath', help='output folder path', type=str, default='.')
parser.add_argument('-s', '--batchsize', help='Batch size for files', type=int, default=100000)
parser.add_argument('-r', '--raw-name', help='filed for raw text data', type=str, default=None)
parser.add_argument('-m', '--parallel', help='Number of cores for parallel processing, 1 is single core processing', type=int, default=1)
args = parser.parse_args()

if args.rename is not None:
    with open(args.rename, 'r') as f:
        renaming = yaml.load(f)
        renaming = [(i[0], i[1]) for i in renaming]
else:
    renaming = args.rename

if args.depth_rename is not None:
    with open(args.depth_rename, 'r') as f:
        ad = yaml.load(f)
else:
    ad = []
        
json_files = os.listdir(args.path)
json_files = [os.path.join(args.path, i) for i in json_files if i.endswith('json')]

if not os.path.exists(args.outpath):
    os.makedirs(args.outpath)

if len(json_files) > args.batchsize:
    start = 0
    file_num = 0
    for end in range(args.batchsize, len(json_files) + args.batchsize,args.batchsize):
        if end >= len(json_files):
            end = len(json_files) - 1
        if args.parallel > 1:
            all_data = Parallel(n_jobs=args.parallel)(delayed(process_json)(each, ad, renaming) for each in json_files[start:end])
        else:
            all_data = []
            for each in json_files[start:end]:
                all_data.append(process_json(each, ad, renaming))
        df = pd.DataFrame(all_data)
        if args.raw_name is not None:
            df[args.raw_name] = [json.dumps(open(jf).read()) for jf in json_files[start:end]]
        del all_data
        df.to_csv(os.path.join(args.outpath, args.filename + '-' + str(file_num) + '.csv'), index=False, quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
        del df
        file_num += 1
        start = end + 1
else:
    if args.parallel > 1:
        all_data = Parallel(n_jobs=args.parallel)(delayed(process_json)(each, ad, renaming) for each in json_files)
    else:
        all_data = []
        for each in json_files:
            all_data.append(process_json(each, ad, renaming))

    df = pd.DataFrame(all_data)
    if args.raw_name is not None:
        df[args.raw_name] = [json.dumps(open(jf).read()) for jf in json_files]
    df = df[sorted(df.columns)]
    del all_data

df.to_csv(os.path.join(args.outpath, args.filename + '.csv'), index=False, quoting=csv.QUOTE_NONNUMERIC, escapechar='\\')
