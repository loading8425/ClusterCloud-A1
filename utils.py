import json


# load sal data
sal_filename = "sal.json"
with open(sal_filename, 'r') as f:
    sal_data = json.load(f)

def process_data(data):
    pass

def compare_city_name(place: str):
    p = place.lower().split(',')
    if p[0] in sal_data.keys():
        gcc = sal_data[p[0]]["gcc"]
        return gcc
    elif len(p) > 1:
        if p[1]==' melbourne' or p[1]==' victoria':
            p[0] = p[0]+' (vic.)'
        elif p[1]==' new south wales':
            p[0] = p[0]+' (nsw)'
        elif p[1]==' queensland':
            p[0] = p[0]+' (qld)'
        elif p[1]==' south australia':
            p[0] = p[0]+' (sa)'
            
        if p[0] in sal_data.keys():
            gcc = sal_data[p[0]]['gcc']
            return gcc
    return -1
