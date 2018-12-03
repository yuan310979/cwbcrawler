import requests
import re
import pickle

from pathlib import Path
from typing import List, Tuple, Dict
from pprint import pprint as pp
from tqdm import tqdm
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date

def clean_str(x):
    x = re.sub('[\t\n\r\xa0 ]+', '', x)
    return x

def get_weather_stations_list() -> List[Tuple[str, str, str, str]]:
    url = 'http://e-service.cwb.gov.tw/HistoryDataQuery/QueryDataController.do?command=viewMain'
    r = requests.get(url)
    l = r.text.split('var stList = {')[1].split('}')[0]
    l = clean_str(l)
    l = re.findall(r'\"([^\"]*)\":\[\"([^\"]*)\",\"([^\"]*)\",\"([^\"]*)\","([^\"]*)\"\]', l)
    return l

def get_weather_stations_detail(station_id):
    url = f'http://e-service.cwb.gov.tw/HistoryDataQuery/QueryDataController.do?command=doQueryStation&station_no={station_id}' 
    r = requests.post(url)
    l = r.text.strip(' \r\n')
    l = l.split('|')[:-1]
    return l

def build_weather_stations_data() -> Dict[str, Dict[str, str]]:
    ws_data = {}
    ws_list = get_weather_stations_list()
    for ws in tqdm(ws_list):
        # ('466920', '臺北', 'TAIPEI', '臺北市', '1')
        station_id = ws[0]

        # ['臺北', '466920', '121.5148', '25.0376', '6.255m', '臺北市', '中正區公園路64號', '中央氣象局', 'cwb', 'TAIPEI', '466920', '1896/01/01'] 
        ws_d = get_weather_stations_detail(station_id)
        assert(len(ws_d) == 12)
        
        try:
            assert(ws[0] == ws_d[1])
            assert(ws[1] == ws_d[0])
            assert(ws[2].replace("&#039;", "\'").strip() == ws_d[9].replace(" ", ""))
            assert(ws[3] == ws_d[5])
        except Exception as ex:
            print(ex)
            print(ws, ws_d)

        ws_f = {
            "city": ws[3],
            "ch_name": ws[1],
            "en_name": ws_d[9],
            "address": ws_d[6],
            "lat": ws_d[3],
            "lon": ws_d[2],
            "alt": ws_d[4],
            "station_type": ws[4],
            "setup_date": ws_d[11]
        }

        ws_data[station_id] = ws_f
    return ws_data

def get_stations_id(ws_data: Dict[str, Dict[str, str]], ws_type: str) -> List[str]:
    if ws_type == "main":
        return [ws for ws in ws_data if ws[:2] == '46']
    elif ws_type == "weather":
        return [ws for ws in ws_data if ws[:2] == 'C0']
    elif ws_type == "rain":
        return [ws for ws in ws_data if ws[:2] == 'C1']
    else:
        return []

def get_single_weather_data(station_id: str, dt: datetime) -> Dict[int, List[str]]:
    time_str = dt.strftime('%Y-%m-%d')
    url = f'https://e-service.cwb.gov.tw/HistoryDataQuery/DayDataController.do?command=viewMain&station={station_id}&stname={station_id}&datepicker={time_str}'
    r = requests.post(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    tb = soup.find('table', {"id": "MyTable"})
    
    ret = {} 
    for index, row in enumerate(tb.find_all('tr')[3:]):
        ret[index+1] = []
        for col in row.find_all('td'):
            val = clean_str(col.text) if clean_str(col.text) != '...' else None
            ret[index+1].append(val)
        assert len(ret[index+1]) == 17 
    assert len(ret) == 24
    return ret

def get_weather_data(st_time: datetime, ed_time: datetime, station_id: str) -> Dict[datetime, Dict[int, List[str]]]:
    ret = {}
    with tqdm(total=(ed_time-st_time)/timedelta(days=1)) as pbar:
        while st_time != ed_time:
            try:
                ret[st_time] = get_single_weather_data(station_id, st_time)
                st_time += timedelta(days=1)
                pbar.update(1)
            except Exception as ex:
                print(ex)
    return ret

if __name__ ==  '__main__':
    data_path = Path('./data/')
    stations_info_path = data_path / 'stations_info.pickle'

    if not data_path.exists(): 
        data_path.mkdir()

    # Store stations info 
    """
    stations_info = build_weather_stations_data()
    stations_info_path.write_bytes(pickle.dumps(stations_info))
    """

    # Load station info 
    stations_info = pickle.load(stations_info_path.open('rb'))
    g = get_stations_id(stations_info, 'main')

    dt_start = date(2018, 9, 28)
    dt_end = date(2018, 10, 2)
    get_weather_data(dt_start, dt_end, g[0])
