import numpy as np
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta, date
import matplotlib.pyplot as plt
import matplotlib

matplotlib.style.use('ggplot')
#matplotlib.rc('xtick', labelsize=20) 
#matplotlib.rc('ytick', labelsize=20)

dow_dict = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 
                4: 'Friday', 5: 'Saturday', 6: 'Sunday'}
dow_dict_reverse = {'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3,
                    'Friday': 4, 'Saturday': 5, 'Sunday': 6}

def get_file_names(start, end):
    """
    Use start and end collection dates to find the valid 
    MTA .txt files to read the data from.
    """
    list_of_files = []
    start_dow = start.weekday()
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
    df2['dayofweek'] = df2['datetime'].map(datetime.weekday).map(lambda x: dow_dict[x])
    df2['date'] = df.date
    df2['time'] = df.time
    df2['entries'] = df.deltaEntries
    df2['exits'] = df.deltaExits
    df2 = df2[df2.entries < 5000]
    df2 = df2[df2.entries >= 0]
    df2 = df2[df2.exits < 5000]
    df2 = df2[df2.exits >= 0]
    return df2

def filter_times(df, start = 12, end = 23, day_or_end = 'all'):
    """
    Returns all entries between start and end time, inclusive.
    """
    if day_or_end == 'd':
        df = df[(df['dayofweek'] != 'Saturday') & (df['dayofweek'] != 'Sunday')]
    elif day_or_end == 'e':
        df = df[(df['dayofweek'] == 'Saturday') | (df['dayofweek'] == 'Sunday')]
    filtered = df[df['datetime'].apply(lambda x: x.hour >= start and 
                                       x.hour < end)]
    return filtered

def n_busiest_stations(df, n):
    """
    Returns the n largest occurrences of total exits
    and the stations at which they occur.
    """
    s= df.groupby('station')['exits'].agg(np.sum) / 1000.0
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

def biggest_weekdays(df):
    """
    Returns the largest days of the week for total exits over
    all stations.
    """
    s = df.groupby('dayofweek')['exits'].agg(np.sum)
    return s.nlargest(7)

def n_biggest_weekdays_by_station(df, n):
    """
    Returns the n largest cumulative days of the week for 
    any station in the given time frame.
    """
    s = df.groupby(['station', 'dayofweek'])['exits'].agg(np.sum)
    return s.nlargest(n)

def n_biggest_stations_on_mday(df, m, n):
    s = df[df.dayofweek == dow_dict[m]].groupby('station')['exits'].agg(np.sum) / 1000.0
    return s.nlargest(n)

def plot_weekdays_for_stn(df, stn, weeks):
    df = df[df.station == stn]
    s = df.groupby('dayofweek')['exits'].agg(np.sum).nlargest(5) / weeks
    s = pd.DataFrame(s)
    new_indices = map(lambda x: dow_dict_reverse[x], s.index)
    s = s.set_index([new_indices])
    s = s.sort_index()
    s.plot()
    plt.xticks(range(5), [dow_dict[i] for i in range(5)], rotation = 60)
    plt.ylim(ymin = 0)
    plt.title(stn[0] + ' counts by weekday')
    plt.tight_layout()
    plt.savefig(stn[0].replace(' ', '_') + '_days.png')
    return s

def plot_barh(s):
    s = pd.DataFrame(s)
    new_indexes = map(lambda x: x[0], s.index)
    s = s.set_index([new_indexes])
    s.plot(kind = 'barh')
    plt.gca().invert_yaxis()
    plt.xlabel('Cumulative exits (x1000)', fontsize = 14)
    plt.ylabel('(station, lines)', fontsize = 18)
    plt.xticks(fontsize = 15)
    plt.yticks(fontsize = 15)
    plt.tight_layout()
    plt.savefig('busiest_stations.png')

def plot_all_by_weekday(df, best, weeks):
    plots = []
    for stn in best:
        plots.append(plot_weekdays_for_stn(df, stn, weeks))
    full_frame = pd.concat(plots, axis = 1)
    full_frame.columns = list(zip(*best)[0])
    full_frame.plot()
    plt.xticks(range(5), [dow_dict[i] for i in range(5)], rotation = 60)
    plt.ylim(ymin = 0)
    plt.title('Top stations exit count by weekday')
    plt.tight_layout()
    plt.savefig('all_stns_by_weekday.png')

def main():
    pd.set_option('display.max_rows', 100)
    pd.set_option('display.width', 200)
        
    start = date(2015, 3, 1)
    end = date(2015, 3, 10)
    files = get_file_names(start, end)
    frames = [read_file(file) for file in files]
    num_weeks = len(files)
    
    big = pd.concat(frames, ignore_index = True)
    big = big.dropna(subset = ['entries', 'exits'])
    big = makeCols(big)
    big = filter_times(big, 16, 20)
    print big.head(100)
    busy_stations = n_busiest_stations(big, 100)
    print busy_stations
    
    """
    busy_days = n_biggest_days_by_station(big, 100)
    print busy_days
    busy_hours = n_biggest_timestamps_by_station(big, 30)
    print busy_hours
    busy_days_of_week = biggest_weekdays(big)
    print busy_days_of_week
    busy_days_of_week_by_station = n_biggest_weekdays_by_station(big, 50)
    print busy_days_of_week_by_station    
    busy_monday_stations = n_biggest_stations_on_mday(big, 0, 10)
    print 'MONDAYS:\n', busy_monday_stations    
    busy_tuesday_stations = n_biggest_stations_on_mday(big, 1, 10)
    print 'TUESDAYS:\n', busy_tuesday_stations
    busy_wednesday_stations = n_biggest_stations_on_mday(big, 2, 10)
    print 'WEDNESDAYS:\n', busy_wednesday_stations
    busy_thursday_stations = n_biggest_stations_on_mday(big, 3, 10)
    print 'THURSDAYS:\n', busy_thursday_stations
    busy_friday_stations = n_biggest_stations_on_mday(big, 4, 10)
    print 'FRIDAYS:\n', busy_friday_stations
    busy_saturday_stations = n_biggest_stations_on_mday(big, 5, 10)
    print 'SATURDAYS:\n', busy_saturday_stations
    busy_sunday_stations = n_biggest_stations_on_mday(big, 6, 10)
    print 'SUNDAYS:\n', busy_sunday_stations
    """

if __name__ == '__main__':
    main()
