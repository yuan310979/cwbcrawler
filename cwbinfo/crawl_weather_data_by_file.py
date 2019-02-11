import pickle

from tqdm import tqdm
from pathlib import Path
from sas7bdat import SAS7BDAT
from datetime import datetime, timedelta

def read_sas7bdat_file(path):
    ret = {}
    with SAS7BDAT('../raw_data/cwb_fixhr2000to2010.sas7bdat', skip_header=True) as reader:
        for row in tqdm(reader):
            station_id = row[0]
            dt = datetime.strptime(str(int(row[1]-1)), "%Y%m%d%H")
            time_str = dt.strftime('%Y-%m-%d')
            # GR 全天空日射量
            PRES, TEMP, RH, WDSP, WDDR, RAIN, SUNH, GR = row[2:]
            if ret.get(station_id) == None:
                ret[station_id] = {}
            if ret[station_id].get(dt.date()) == None:
                ret[station_id][dt.date()] = {}
            ret[station_id][dt.date()][dt.hour+1] = [int(dt.hour+1), PRES, None, TEMP, None, RH, WDSP, WDDR, None, None, RAIN, None, SUNH, GR, None, None, None]
    return ret

def save_model(obj, path):
    path = Path(path)
    if not path.parent.exists():
        path.parent.mkdir() 
    path.write_bytes(pickle.dumps(obj))

ret = read_sas7bdat_file('../raw_data/cwb_fixhr2000to2010.sas7bdat')
save_model(ret, '../pickle_data/weather_data(2000-2010).pickle')
