import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta, date

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
    df2 = pd.DataFrame()
    df2['stile'] = zip(df.ca, df.unit, df.scp, df.station)
    df2['datetime'] = pd.to_datetime(df.date+df.time, format = '%m/%d/%Y%H:%M:%S')
    df2['linename'] = df.linename
    df2['date'] = df.date
    df2['time'] = df.time
    df2['entries'] = df.deltaEntries
    df2['exits'] = df.deltaExits
    df2 = df2[df2.entries < 5000]
    df2 = df2[df2.entries >= 0]
    return df2

def filter_times(df, start = 12, end = 23):
    """
    Returns all entries between start and end time, inclusive.
    """
    filtered = df[df['datetime'].apply(lambda x: x.hour >= start and x.hour <= end)]
    return filtered

def main():
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.width', 200)
    list_of_files = []
    list_of_frames = []
    
    currDate = date(2015, 1, 3)
    while currDate < date(2015, 2, 1):
        list_of_files.append('turnstile_' + currDate.strftime('%y%m%d') + '.txt')
        currDate += timedelta(7)
    list_of_frames = [read_file(filename) for filename in list_of_files]
    big = pd.concat(list_of_frames, ignore_index = True)
    big = big.dropna(subset = ['entries', 'exits'])
    big = makeCols(big)
    #print big
    print filtered(big)

if __name__ == '__main__':
    main()
