import pickle
import numpy as np
import multiprocessing as mp

from pathlib import Path
from tqdm import tqdm
from pprint import pprint as pp
from itertools import product
from datetime import datetime, timedelta, date
from cwbinfo import CWBInfo

CWB = CWBInfo()
CWB.load_station_data_from_pickle('../pickle_data/stations_info.pickle')
CWB.load_weather_data_from_pickle('../pickle_data/weather_data.pickle')

start_lon = 120.00
start_lat = 21.80
lon_range = 2
lat_range = 3.5
step = 0.01

"""
start_lon = 121.50
start_lat = 24.00
lon_range = 0.1
lat_range = 0.1
step = 0.1
"""

grids = {}

start_time = date(2015, 1, 1)
end_time = date(2016, 1, 1)

keys = product(np.arange(0, lat_range,step) + start_lat, np.arange(0, lon_range, step) + start_lon)
keys = [(round(key[0], 2), round(key[1], 2)) for key in keys] 

error_state = [None, 'V', 'v', 'X', 'x', 'T', 't', '/']

for key in tqdm(keys):
    f = [[], [], [], [], []]
    st_name = CWB.get_nearest_weather_station(key[0], key[1])
    d = CWB.get_meteo_data_in_period(st_name, start_time, end_time)
    for record in d:
        for i in range(1, 25):
            f[0].append(float(record[i][3]) if record[i][3] not in error_state else 0)
            f[1].append(float(record[i][1]) if record[i][1] not in error_state else 0)
            f[2].append(float(record[i][5]) if record[i][5] not in error_state else 0)
            f[3].append(float(record[i][6]) if record[i][6] not in error_state else 0)
            f[4].append(float(record[i][7]) if record[i][7] not in error_state else 0)
    f = np.array(f)
    f[0] = (f[0]-min(f[0])) / (max(f[0])-min(f[0])) if max(f[0])-min(f[0]) != 0 else 0
    f[1] = (f[1]-min(f[1])) / (max(f[1])-min(f[1])) if max(f[1])-min(f[1]) != 0 else 0 
    f[2] = (f[2]-min(f[2])) / (max(f[2])-min(f[2])) if max(f[2])-min(f[2]) != 0 else 0 
    f[3] = (f[3]-min(f[3])) / (max(f[3])-min(f[3])) if max(f[3])-min(f[3]) != 0 else 0 
    f[4] = (f[4]-min(f[4])) / (max(f[4])-min(f[4])) if max(f[4])-min(f[4]) != 0 else 0 
    grids[key] = f
    
# pp(grids)

data_path = Path('../input_feature/meteo.pickle')

if not data_path.parent.exists():
    data_path.parent.mkdir()

data_path.write_bytes(pickle.dumps(grids))

"""
d = pickle.load(data_path.open('rb'))
pp(d)
"""
