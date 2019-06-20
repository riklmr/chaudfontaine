import os
import pandas as pd
import pickle


"""
Let's replace the whole aweful multi-indexed dataframe with a dict using a serialized key.
i.e.  'debit-5291-2011-7'

Then store the dict in a pickle. PoC below.
"""


DATA_COVERAGE_FILENAME = 'data_coverage.csv'
DATA_COVERAGE_PICKLENAME = 'data_coverage.pickle'

data_coverage = pd.read_csv(DATA_COVERAGE_FILENAME)
data_coverage.set_index(['station_type', 'station_code', 'year', 'month'], inplace=True)

daco_dict = {}

for i in data_coverage.index:
    co = data_coverage.loc[i, 'coverage']
    iiii = "{}-{}-{}-{}".format(i[0], i[1], i[2], i[3])
    # print(iiii, ":", co)
    daco_dict[iiii] = co

print(daco_dict)

daco_pickle = open(DATA_COVERAGE_PICKLENAME, mode='wb')
pickle.dump(daco_dict, daco_pickle)
daco_pickle.close()


daco_pickle = open(DATA_COVERAGE_PICKLENAME, mode='rb')
daco_dict = pickle.load(daco_pickle)
daco_pickle.close()

print(daco_dict)
