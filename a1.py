#!/usr/bin/python
# -*- coding: utf8 -*-

'''
COMP9321 Assignment One Code Template 2019T1
Name: Taiyan Zhu	
Student ID: z5089986
'''

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.image as img
import re
import json
import gc
from decimal import *

import time
from functools import wraps

getcontext().prec = 50

#(llcrnrlon, llcrnrlat) = (1.92035661123186, 41.27929274916819)#unproject(31, "T", 409584, 4570324)
#(urcrnrlon, urcrnrlat) = (2.421312545644872, 41.497254141512116)#nproject(31, "T", 451699, 4594121)
(llcrnrlon, llcrnrlat) = (1.9168051379804014, 41.28291056330658)#unproject(31, "T", 409584, 4570324)
(urcrnrlon, urcrnrlat) = (2.423210210294655, 41.49360912228953)#nproject(31, "T", 451699, 4594121)

#lon_x =  urcrnrlon - llcrnrlon
#lat_y = urcrnrlat-llcrnrlat

def to_lon(_lon, x):

    return float(Decimal(_lon-llcrnrlon)/Decimal((Decimal(urcrnrlon)-Decimal(llcrnrlon))/Decimal(x)))

def to_lat(_lat, y):
    return float(y) - float(Decimal(_lat-llcrnrlat)/Decimal((Decimal(urcrnrlat) - Decimal(llcrnrlat))/Decimal(y)))

list_month = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]

def to_month(s):
    month = int(s.strip().split()[0].split("/")[1])
    return list_month[month-1]

def to_hour(s):
    return int(s.strip().split()[1].split(":")[0])

def to_day(s):
    return int(s.strip().split()[0].split("/")[0])


def is_ignore(s):
    ignore_w_list = ["la", "de"]
    ignore_pre_list = ["l'", "d'"]
    res = False
    for ignore_word in ignore_w_list:
        if (s == ignore_word):
            res = True
            break
    if (res == False):
        for ignore_prefix in ignore_pre_list:
            if s.startswith(ignore_prefix):
                res = True
                break
    return res

def to_title_style(item):
    res = []
    s = str(item)
    word_list = re.split(' ', s.strip())
    for word in word_list:
        if is_ignore(word):
            res.append(word)
        else:
            res.append(word.title())
    return " ".join(res)

def q1():

    csv_data = pd.read_csv('accidents_2017.csv').head(10)
    pd.set_option('display.max_columns', None)
    for col in csv_data.columns:
        if ' ' in col:
            col = '"' + col + '"'
        print(col, end=' ')
    print("")

    for row in csv_data.values:
        _list = []
        for col in row:
            col = to_title_style(col)
            if ' ' in col:
                col = '"' + col + '"'
            _list.append(col)
        print(" ".join(_list))

    pass

def q2():
    csv_data = pd.read_csv('accidents_2017.csv')

    del_index = []

    #csv_data = csv_data.head(10)
    max = len(csv_data)
    for row in range(max):
        for col in csv_data.columns:
            item = csv_data.at[row, col]
            if ((not isinstance(item, str)) and (not isinstance(item, list))): continue
            if 'unknown' == item.strip().lower() or '-' == item.strip().lower():
                del_index.append(row)
                break
            csv_data.at[row, col] = to_title_style(item)

    csv_data.drop(del_index, axis=0, inplace=True)

    csv_data.to_csv(r"result_q2.csv", index=False)

    pass

def q3():
    csv_data = pd.read_csv('accidents_2017.csv')

    del_index = []

    #csv_data = csv_data.head(10)

    max = len(csv_data.index)

    for row in range(max):
        for col in csv_data.columns:
            item = csv_data.at[row, col]
            if ((not isinstance(item, str)) and (not isinstance(item, list))): continue
            if 'unknown' == item.strip().lower() or '-' == item.strip().lower():
                del_index.append(row)
                break
            csv_data.at[row, col] = to_title_style(item)

    csv_data.drop(del_index, axis=0, inplace=True)

    csv_data = csv_data.drop_duplicates()

    csv_data['Total numbers of accidents'] = 1

    statistic_data = csv_data.groupby("District Name").sum()

    district_data = pd.DataFrame(statistic_data["Total numbers of accidents"])

    head = ["\"District Name\"", "\"Total numbers of accidents\""]

    print(*head)
    #print(head[0], head[1])

    district_data = district_data.sort_values(by = 'Total numbers of accidents',axis = 0,ascending = False)


    for row in district_data.index:
        item = row
        if ' ' in row:
            item = '"' + row + '"'
        print(item, district_data.at[row, "Total numbers of accidents"])

    pass

def q4():

    air_sta_data = pd.read_csv('air_stations_Nov2017.csv')

    json_list = []
    for row in air_sta_data.index:
        obj = {}
        obj["Station"] = str(air_sta_data.at[row, 'Station'])
        obj["District Name"] = str(air_sta_data.at[row, 'District Name'])
        #row_data = json.dumps(obj, separators=(',', ':'))
        json_list.append(obj)
    print(json_list)
    print("")

    air_qly_data = pd.read_csv('air_quality_Nov2017.csv')
    air_qly_data = air_qly_data.drop_duplicates()
    air_qly_data = air_qly_data[(air_qly_data["Air Quality"] != "Good") & (air_qly_data["Air Quality"] != "--")]

    for col in air_qly_data.columns:
        if ' ' in col:
            col = '"' + col + '"'
        print(col, end=' ')
        #print col
    print("")

    for row in air_qly_data.head(10).values:
        _list = []
        for col in row:
            col = to_title_style(col)
            if ' ' in col:
                col = '"' + col + '"'
            _list.append(col)
        print(" ".join(_list))

    air_qly_data["Hour"] = air_qly_data["Generated"].apply(lambda x: to_hour(str(x)))
    air_qly_data["Day"] = air_qly_data["Generated"].apply(lambda x: to_day(str(x)))
    air_qly_data["Month"] = air_qly_data["Generated"].apply(lambda x: to_month(str(x)))

    air_qly_data = pd.DataFrame(air_qly_data.loc[:,['Station', 'Hour', 'Day', 'Month']])
    air_sta_data = pd.DataFrame(air_sta_data.loc[:,["Station", "District Name"]])

    air_data = pd.DataFrame(pd.merge(air_sta_data, air_qly_data, how='inner').loc[:,['District Name', 'Hour', 'Day', 'Month']])

    del air_qly_data
    del air_sta_data
    gc.collect()

    csv_data = pd.read_csv('accidents_2017.csv')

    csv_data = pd.DataFrame(pd.merge(csv_data, air_data, on=['District Name', 'Hour', 'Day', 'Month'], how="inner").loc[:,csv_data.columns])



    del_index = []

    max = len(csv_data)
    for row in range(max):
        for col in csv_data.columns:
            item = csv_data.at[row, col]
            if ((not isinstance(item, str)) and (not isinstance(item, list))): continue
            if 'unknown' == item.strip().lower() or '-' == item.strip().lower():
                del_index.append(row)
                break
            csv_data.at[row, col] = to_title_style(item)

    csv_data.drop(del_index, axis=0, inplace=True)

    csv_data.to_csv(r"result_q4.csv", index=False)

    pass

def q5():
    im = img.imread('map.png')

    (y, x, z) = im.shape
    #plt.figure(figsize=(12,12))
    plt.imshow(im)

    csv_data = pd.read_csv('accidents_2017.csv')
    csv_data = csv_data.loc[(csv_data["District Name"] != "Unknown") & (csv_data["Neighborhood Name"] != "Unknown"), ["Longitude", "Latitude"]]
    #csv_data = csv_data[(csv_data["District Name"] != "Unknown") and (csv_data["Neighborhood Name"] != "Unknown")]
    #csv_data = csv_data[]
    #csv_data = pd.DataFrame(csv_data.loc[:, ["Longitude", "Latitude"]])


    lon = csv_data['Longitude'].values
    lat = csv_data['Latitude'].values

    lon = [to_lon(e, x) for e in lon]
    lat = [to_lat(e, y) for e in lat]

    #plt.xlim([0, x])
    #plt.ylim([x, 0])
    pd.DataFrame

    plt.scatter(lon, lat, marker='.', s=3, c='tomato', alpha=0.5)

    fig = plt.gcf()
    fig.set_size_inches(float(x) / 100, float(y) / 100)
    fig.tight_layout()
    plt.gca().xaxis.set_major_locator(plt.NullLocator())
    plt.gca().yaxis.set_major_locator(plt.NullLocator())
    plt.subplots_adjust(top=1, bottom=0, right=1, left=0, hspace=0, wspace=0)
    plt.margins(0, 0)
    # fig.savefig('plot.png', format='png', transparent=True, dpi=300, pad_inches=0)
    # plt.savefig('test.png',bbox_inches='tight')

    plt.savefig('plot.png')
    plt.show()

    pass

'''def print_func_time(function):

    @wraps(function)
    def func_time(*args, **kwargs):
        t0 = time.clock()
        result = function(*args, **kwargs)
        t1 = time.clock()
        print("Total running time: %s s" % (str(t1 - t0)))
        return result

    return func_time

@print_func_time
def test():
    q4()

test()
'''

q1()
q2()
q3()
q4()
q5()