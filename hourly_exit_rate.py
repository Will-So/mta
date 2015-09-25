# -*- coding: utf-8 -*-
"""
Created on Thu Sep 24 11:37:03 2015

@author: gregoryfriedman
"""

import numpy as np
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta, date

def get_file_names(start, end):
    list_of_files = []
    start_dow = start.weekday()
    #print start_dow
    start += (timedelta((5 - start_dow) % 7))
    while start < end:
        list_of_files.append('../turnstile_data_2015/turnstile_' + start.strftime('%y%m%d') + '.txt')
        start += timedelta(7)
    return list_of_files

def read_file(filename):
    cols = ['ca', 'unit', 'scp', 'station', 
            'linename', 'division', 'date', 'time', 
            'desc', 'entries', 'exits']
    traffic = pd.read_csv(filename, 
                          header = True, 
                          names = cols, 
                          converters = {'linename': lambda x: ''.join(sorted(x))})
    #print traffic.head()
    return traffic

def makeCols(df):
    df['deltaEntries'] = df.groupby(['ca', 'unit', 'scp', 'station', ]).entries.diff()
    df['deltaExits'] = df.groupby(['ca', 'unit', 'scp', 'station']).exits.diff()
    df = clean_frame(df)
    return df

def clean_frame(df):
    df2 = pd.DataFrame()
#    df2['stile'] = zip(df.ca, df.unit, df.scp, df.station)
    df2['station'] = zip(df.station, df.linename)
    df2['datetime'] = pd.to_datetime(df.date+df.time, format = '%m/%d/%Y%H:%M:%S')
    df2['date'] = df.date
    df2['time'] = df.time
    df2['entries'] = df.deltaEntries
    df2['exits'] = df.deltaExits
    df2 = df2[df2.entries < 5000]
    df2 = df2[df2.entries >= 0]
    df2 = df2[df2.exits < 5000]
    df2 = df2[df2.exits >= 0]
    return df2

def filter_times(df, start = 12, end = 20):
    """
    Returns all entries between start and end time, inclusive.
    """
    filtered = df[df['datetime'].apply(lambda x: x.hour >= start and x.hour < end)]
    return filtered


def aggregate_turnstiles(df):
    '''
    sum the turnstiles for each station for each count"
    '''
    count = df.groupby(['station', 'date', 'datetime'])['exits'].sum().reset_index()
    return count

def max_peaks(df, n):
    s = aggregate_turnstiles(df)
    
    daily_max = s.groupby(level = ('station')).max()
    return daily_max.nlargest(n)
    #maximum = s.groupby(level = 'station').max()
    #return maximum.nlargest(n)

def station_timelines(df):
    return df.reset_index(pd.DatetimeIndex(df['datetime']))
    
def time_deltas(df):
    df['delta'] = (df['datetime']-df['datetime'].shift()).fillna(0)
    df['delta'] = df['delta'].apply(lambda x: x  / np.timedelta64(1,'h')).astype('float64')
    df = df[df.delta > 1]
    return df
        
def hourly_exits(df):
    df = station_timelines(df)
    print df[:100]
    print type(df)
    df = time_deltas(df)
    print df[2000:2100]
    df['exitshourly'] = (df['exits']/df['delta']).astype('int64')
    print type(df)
    df.reset_index()
    print type(df)
    return df

def daily_exit_rate(df):
    #return df.groupby(['station', 'date'])['exitshourly'].agg(np.mean)
    by_station = df.groupby(['station', 'date']).agg({'exitshourly': np.mean, 'exits': np.sum})
    return by_station

def exits_by_day(df):
    by_date = df.groupby(['date', 'station']).agg({'exitshourly': np.mean, 'exits': np.sum})
    return by_date

def busiest_exits(df, n):
    s= df.groupby('station')['exitshourly'].agg(np.mean)
    return s.nlargest(n)
    
def n_busiest_stations(df, n):
    s= df.groupby('station')['exits'].agg(np.sum)
    return s.nlargest(n)

def n_biggest_days_by_station(df, n):
    s= df.groupby(['station', 'date'])['exits'].agg(np.sum)
    return s.nlargest(n)

def n_biggest_timestamps_by_station(df, n):
    s= df.groupby(['station', 'date', 'time'])['exits'].agg(np.sum)
    return s.nlargest(n)

def main():
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.width', 200)

    list_of_files = []
    list_of_frames = []
    
    currDate = date(2015, 1, 3)
    while currDate < date(2015, 2, 1):
        list_of_files.append('../turnstile_data_2015/turnstile_' + currDate.strftime('%y%m%d') + '.txt')
        currDate += timedelta(7)
    list_of_frames = [read_file(filename) for filename in list_of_files]
    big = pd.concat(list_of_frames, ignore_index = True)


    
    start = date(2015, 3, 1)
    end = date(2015, 6, 1)
    files = get_file_names(start, end)
    frames = [read_file(file) for file in files]
    big = pd.concat(frames, ignore_index = True)
    big = big.dropna(subset = ['entries', 'exits'])
    big = makeCols(big)
    print big[400:500]
    station_counts = aggregate_turnstiles(big)
    station_rates = hourly_exits(station_counts)
    print station_rates[:100]
    daily_exits = daily_exit_rate(station_rates)
    station_list = exits_by_day(station_rates)
    print daily_exits[3000:3100]
    print station_list[3000:3100]
    print daily_exits.nlargest(50)
    print station_list.nlargest(50)
    #peak = filter_times(big)
    #peak1 = peak_traffic(peak, 50)
    #maxes = max_peaks(peak, 50)
    #print maxes
    #print peak1
    #busy_stations = n_busiest_stations(peak, 50)
    #print busy_stations
    #n_biggest_days_by_station(peak, 100)
    #busy_times = n_biggest_timestamps_by_station(peak, 100)
    #print busy_times
    #print big.station.unique()
    #timeline = station_timelines(big)
    #print timeline[400:500]
    #elapsed = time_deltas(timeline)
    #peak_elapsed = filter_times(elapsed)
    #print elapsed[400:500]
    #exit_rate = hourly_exits(peak_elapsed)
    #print exit_rate[5000:5100]
    #busy_exits = busiest_exits(peak_elapsed, 50)
    #print busy_exits
    
    

if __name__ == '__main__':
    main()
