import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
from datetime import *
import turnstiles
import pandas as pd
import numpy as np

def plot_weekdays_for_stn(df, stn, weeks, n = 5):
    df = df[df.station == stn]
    s = df.groupby('dayofweek')['exits'].agg(np.sum).nlargest(n) / weeks
    s = pd.DataFrame(s)
    new_indices = map(lambda x: turnstiles.dow_dict_reverse[x], s.index)
    s = s.set_index([new_indices])
    s = s.sort_index()
    s.plot()
    plt.xticks(range(n), [turnstiles.dow_dict[i] for i in range(n)], rotation = 60)
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

def plot_all_by_weekday(df, best, weeks, n):
    plots = []
    for stn in best:
        plots.append(plot_weekdays_for_stn(df, stn, weeks, 5))
    full_frame = pd.concat(plots, axis = 1)
    full_frame.columns = list(zip(*best)[0])
    full_frame.plot()
    plt.xticks(range(5), [turnstiles.dow_dict[i] for i in range(5)], rotation = 60)
    plt.ylim(ymin = 0)
    plt.title('Top stations exit count by weekday (average over Mar 1 - Jun 1)')
#    if n == 'All':
#        plt.gca().legend_ =  None
#    else:
#        plt.legend(bbox_to_anchor=(0., .11, -0.5, .102), loc = 3, ncol = 2, prop = FontProperties().set_size('small'))
    plt.tight_layout()
    plt.savefig('all_stns_by_weekday' + str(n) + '.png')

def plot_all_by_week(df, best, weeks, n):
    plots = []
    for stn in best:
        plots.append(plot_weekdays_for_stn(df, stn, weeks, 7))
    full_frame = pd.concat(plots, axis = 1)
    full_frame.columns = list(zip(*best)[0])
    full_frame.plot()
    plt.xticks(range(7), [turnstiles.dow_dict[i] for i in range(7)], rotation = 60)
    plt.ylim(ymin = 0)
    plt.title('Top stations exit count by weekday/weekend (average over Mar 1 - Jun 1)')
#    plt.gca().legend_ =  None
    plt.legend(prop = FontProperties().set_size('small'))
    plt.tight_layout()
    plt.savefig('all_stns_by_weekday' + str(n) + '.png')



def main():
    files = turnstiles.get_file_names(date(2015, 3, 1), date(2015, 6, 1))
    frames = [turnstiles.read_file(file) for file in files]
    num_weeks = len(files)
    big = pd.concat(frames, ignore_index = True)
    big = big.dropna(subset = ['entries', 'exits'])
    big = turnstiles.makeCols(big)
    big = turnstiles.filter_times(big, 16, 20)
    bigD = turnstiles.filter_times(big, 16, 20, 'd')
    busy_stationsD = turnstiles.n_busiest_stations(bigD, 20)
    bigE = turnstiles.filter_times(big, 16, 20, 'e')

    plot_barh(busy_stationsD)

    
    best_stns = [('14 ST-UNION SQ', '456LNQR'), ('34 ST-PENN STA', '123'), ('42 ST-GRD CNTRL', '4567S')]
    middle_stns = [('34 ST-HERALD SQ', 'BDFMNQR'), ('59 ST-COLUMBUS', '1ABCD'), ('86 ST', '456')]
    low_stns = [('W 4 ST-WASH SQ', 'ABCDEFM'), ('72 ST', '123'), ('BARCLAYS CENTER', '2345BDNQR')]
    plot_all_by_weekday(bigD, best_stns, num_weeks, 1)
    plot_all_by_weekday(bigD, middle_stns, num_weeks, 2)
    plot_all_by_weekday(bigD, low_stns, num_weeks, 3)
    plot_all_by_weekday(bigD, best_stns + middle_stns + low_stns, num_weeks, 'All')
    plot_all_by_week(big, best_stns + middle_stns + low_stns, num_weeks, 'AllWeek')
    

if __name__ == '__main__':
    main()
