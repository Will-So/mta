import numpy as np
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta, date

def get_file_names(start, end):
    """
    Use start and end collection dates to find the valid 
    MTA .txt files to read the data from.
    """
    list_of_files = []
    start_dow = start.weekday()
    print start_dow
    start += (timedelta((5 - start_dow) % 7))
    while start < end:
        list_of_files.append('turnstile_' + start.strftime('%y%m%d') + '.txt')
        start += timedelta(7)
    return list_of_files

def read_file(filename):
    """
    Read the given filename into a pandas dataframe. Alphabetize
    the line names to catch permutations.
    """
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
    """
    Transform the cumulative total counts into absolute counts
    per timestamp for both entries and exits. Also clean frame.
    """
    df['deltaEntries'] = df.groupby(['ca', 'unit', 
                                     'scp', 'station']).entries.diff()
    df['deltaExits'] = df.groupby(['ca', 'unit', 
                                   'scp', 'station']).exits.diff()
    df = clean_frame(df)
    return df

def clean_frame(df):
    """
    Returns cleaned data frame with station and line name
    zipped into one tuple, and a new datetime column for sorting.
    CA, unit, scp, division and desc are removed from the frame,
    and invalid counts have their rows stripped from the frame. 
    """
    df2 = pd.DataFrame()
    df2['station'] = zip(df.station, df.linename)
    df2['datetime'] = pd.to_datetime(df.date+df.time, 
                                     format = '%m/%d/%Y%H:%M:%S')
    df2['date'] = df.date
    df2['time'] = df.time
    df2['entries'] = df.deltaEntries
    df2['exits'] = df.deltaExits
    df2 = df2[df2.entries < 5000]
    df2 = df2[df2.entries >= 0]
    df2 = df2[df2.exits < 5000]
    df2 = df2[df2.exits >= 0]
    return df2

def filter_times(df, start = 12, end = 23):
    """
    Returns all entries between start and end time, inclusive.
    """
    filtered = df[df['datetime'].apply(lambda x: x.hour > start and 
                                       x.hour <= end)]
    return filtered

def n_busiest_stations(df, n):
    """
    Returns the n largest occurrences of total exits
    and the stations at which they occur.
    """
    s= df.groupby('station')['exits'].agg(np.sum)
    return s.nlargest(n)

def n_biggest_days_by_station(df, n):
    """
    Returns the n largest occurrences of total exits
    per day and the stations at which they occur.
    """
    s= df.groupby(['station', 'date'])['exits'].agg(np.sum)
    return s.nlargest(n)

def n_biggest_timestamps_by_station(df, n):
    """
    Returns the n largest occurrences of total exits
    per timestamp (accumulated over all days) and the 
    stations at which they occur.
    """
    s = df.groupby(['station', 'time'])['exits'].agg(np.sum)
    return s.nlargest(n)

def main():
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.width', 200)
        
    start = date(2015, 3, 1)
    end = date(2015, 6, 1)
    files = get_file_names(start, end)
    frames = [read_file(file) for file in files]
    big = pd.concat(frames, ignore_index = True)
    big = big.dropna(subset = ['entries', 'exits'])
    big = makeCols(big)
    big = filter_times(big, 16, 20)
    busy_stations = n_busiest_stations(big, 10)
    print busy_stations
    busy_days = n_biggest_days_by_station(big, 100)
    print busy_days
    busy_hours = n_biggest_timestamps_by_station(big, 30)
    print busy_hours

if __name__ == '__main__':
    main()
