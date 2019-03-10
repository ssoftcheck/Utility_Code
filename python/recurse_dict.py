import re
import json

def recurse_dict(x, apply_depth=None, name_change=None, level=None, depth=1, data=None):
    if data is None:
        data = {}
    if level is None:
        level = []
    if apply_depth is None:
        apply_depth = []
    if type(x) in (str, unicode):
        level_edit = list(level)
        if name_change is not None:
            in_apply_depth = list(map(lambda x: any([re.match(p,x,re.IGNORECASE) is not None for p in apply_depth]), level_edit))
            for old, new in name_change:
                level_edit = [re.sub(old, new.format(depth=depth), tochange, re.IGNORECASE) if m else tochange for m,tochange in zip(in_apply_depth,level_edit)]
            name = '_'.join([str(item.encode('utf-8')).strip() for item in level_edit if item != ''])
        else:
            name = '_'.join([item.encode('utf-8').strip() for item in level if item != ''])
        if len(name) > 0:
            data[name] = x.encode('utf-8').strip()
    else:
        for k in sorted(x):
            level.append(k)
            level_edit = list(level)
            if name_change is not None:
                for old, new in name_change:
                    level_edit = [re.sub(old, new.format(depth=depth), le, re.IGNORECASE) for le in level_edit]
            if type(x[k]) in (str, unicode, dict):       
                recurse_dict(x[k], apply_depth, name_change, level_edit, depth, data)
            elif type(x[k]) == list:
                incr = 1
                for j in x[k]:
                    recurse_dict(j, apply_depth, name_change, level_edit, incr, data)
                    incr += 1
            level.pop()
    return data.copy()

def process_json(in_file, depth_names, new_names):
    with open(in_file ,'r') as handle:
        try:
            raw = json.load(handle)
        except:
            print('Failed to load JSON file {}'.format(in_file))
            return {}
    try:
            return recurse_dict(raw, depth_names, new_names)
    except:
        print('Error parsing file {}'.format(in_file))
return {}
