import pickle

from process_weather_data import get_stations_id, build_weather_stations_data, get_weather_data
from datetime import datetime, timedelta, date
from pathlib import Path
from tqdm import tqdm

data_path = Path('./data/')
stations_info_path = data_path / 'stations_info.pickle'
weather_data_path = data_path / 'weather_data.pickle'

# download station data to pickle file 
"""
stations_info = build_weather_stations_data()
stations_info_path.write_bytes(pickle.dumps(stations_info))
"""

# load station data from pickle file
stations_info = pickle.load(stations_info_path.open('rb'))

# get all target station id
main_station = get_stations_id(stations_info, 'main')
C0_station = get_stations_id(stations_info, 'weather')
ws = main_station + C0_station

# get all weather data from start_time to end_time
# data: Dict[str, Dict[datatime, Dict[int, List[str]]]]
data = {} 
start_time = date(2015, 1, 1)
end_time = date(2016, 1, 1)
for ws_id in tqdm(ws):
    data[ws_id] = get_weather_data(start_time, end_time, ws_id)
weather_data_path.write_bytes(pickle.dumps(data))
