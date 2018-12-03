import pickle
import re

from math import sin, cos, sqrt, atan2, radians
from pathlib import Path
from tqdm import tqdm
from datetime import datetime, timedelta, date
from pprint import pprint as pp

class CWBInfo:

    def __init__(self):
        self.station_data = None
        self.weather_data = None

    def load_station_data_from_pickle(self, path):
        path = Path(path)
        self.station_data = pickle.load(Path(path).open('rb'))

    def load_weather_data_from_pickle(self, path):
        path = Path(path)
        self.weather_data = pickle.load(Path(path).open('rb'))

    def get_nearest_weather_station(self, lat, lon):
        ret = None
        min_dis = 10 ** 10
        for k, v in self.station_data.items():
            if(k[:2] != 'C1'):
                dis = self.latlon_distance(lat, lon, float(v['lat']), float(v['lon']))
                if dis < min_dis:
                    min_dis = dis
                    ret = k
        return ret

    def get_stations(self):
        return self.station_data

    def get_meteo_data_in_period(self, st_id, st_time, ed_time):
        d = self.weather_data[st_id]
        ret = [] 
        with tqdm(total=(ed_time-st_time)/timedelta(days=1)) as pbar:
            while st_time != ed_time:
                try:
                    ret.append(d[st_time])
                except Exception as ex:
                    print(ex)
                finally:
                    st_time += timedelta(days=1)
                    pbar.update(1)
        return ret

    @staticmethod
    def latlon_distance(lat1:float, lon1:float, lat2:float, lon2:float) -> float:
        # Ref: https://www.movable-type.co.uk/scripts/latlong.html 
        R = 6371.0
        phi1 = radians(lat1)
        phi2 = radians(lat2)
        phi = radians(lat2-lat1)
        sigma = radians(lon2-lon1)

        a = sin(phi/2)**2 + cos(phi1) * cos(phi2) * sin(sigma/2)**2
        c = 2 * atan2(sqrt(a), sqrt(1-a))

        return R * c

if __name__ == "__main__":
    CWB = CWBInfo()
    CWB.load_station_data_from_pickle('../pickle_data/stations_info.pickle')
    CWB.load_weather_data_from_pickle('../pickle_data/weather_data.pickle')

    st = CWB.get_nearest_weather_station(24.32323, 121.456465)
    #print(EPA.get_data_by_station_name(st))
    print(st)
    d = CWB.get_meteo_data_in_period(st, date(2015, 1, 1), date(2015, 2, 1))

    #pp(d)
