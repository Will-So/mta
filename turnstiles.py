import numpy as np
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta, date

def get_file_names(start, end):
    list_of_files = []
    start_dow = start.weekday()
    print start_dow
    start += (timedelta((5 - start_dow) % 7))
    while start < end:
        list_of_files.append('turnstile_' + start.strftime('%y%m%d') + '.txt')
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
    print traffic.head()
    return traffic

def makeCols(df):
    df['deltaEntries'] = df.groupby(['ca', 'unit', 'scp', 'station']).entries.diff()
    df['deltaExits'] = df.groupby(['ca', 'unit', 'scp', 'station']).exits.diff()
    df = clean_frame(df)
    return df

def clean_frame(df):
def clean_frame(df, all_csv=False):
    """Sets the dataframe to the correct type and removes certain erroneous entries

    params
    ----
    df: dataframe populated with the mta subway data. 
    """

    # Removes extra headers that may have been generated while generating yearly csv
    if all_csv:
        df = remove_extra_headers(df)
    
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

def remove_extra_headers(df):
    traffic = traffic.loc[traffic.entries != 'ENTRIES']
    traffic.entries = traffic.entries.astype(int)
    traffic.exits = traffic.exits.astype(int)

def n_biggest_total(df, period, n):
    s= df.groupby(['station', period])['exits'].agg(np.sum)
    print s.nlargest(n)

def main():
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.width', 200)
        
    start = date(2015, 1, 1)
    end = date(2015, 2, 1)
    files = get_file_names(start, end)
    frames = [read_file(file) for file in files]
    big = pd.concat(frames, ignore_index = True)
    big = big.dropna(subset = ['entries', 'exits'])
    big = makeCols(big)
    print filter_times(big)
    n_biggest_total(big, 'date', 100)
    

if __name__ == '__main__':
    main()
