import pickle

from pathlib import Path

def merge(dict1, dict2):
    ret = dict1 
    for st, r in dict2.items():
        for date, record in r.items():
            if ret.get(st) == None:
                ret[st] = {}
            ret[st][date] = record
    return ret

def save_pickle(obj, path):
    path = Path(path)
    if not path.parent.exists():
        path.parent.mkdir() 
    path.write_bytes(pickle.dumps(obj))

def load_pickle(path):
    path = Path(path)
    return pickle.load(Path(path).open('rb'))

pk1 = load_pickle("../pickle_data/weather_data(2000-2010).pickle")
pk2 = load_pickle("../pickle_data/weather_data.pickle")

ret = merge(pk1, pk2)

save_pickle(ret, "../pickle_data/weather_data(2000-2017).pickle")
