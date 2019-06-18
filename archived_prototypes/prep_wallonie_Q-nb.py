#%% [markdown]
# # import Wallonia's Q observations
# ```
# file format
# how to acquire
# reformat
# saved format
# ```

#%%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import glob
import os
import re
get_ipython().run_line_magic('matplotlib', 'inline')


#%%
DATAPATH = "meuse_forecast/datasets/wallonie/"
os.chdir("/home/paperspace")
os.chdir(DATAPATH)
all_stations_csv = glob.glob("*.csv")


#%%
# iterate over all csv files (one for every station)
for csv in sorted(all_stations_csv):
    station_code, observation_code, freq_code = re.match("^(\d\d\d\d)(\d\d\d\d)_(...)\.csv", csv).groups()
    print(station_code, observation_code, freq_code)

    # read csv into pandas: 
    #  separator contains silly space after semicolon\
    #  column 'date' becomes the index (string/object type)
    #  we skip line #0, even though it contains interesting meta info
    raw_df = pd.read_csv(csv, sep=';\s?', engine='python', index_col=['date'], header=1 )
    
    # drop the last, unnamed, column which is the result of a spurious, trailing separator
    raw_df.drop(labels=['Unnamed: 25'], axis=1, inplace=True)
    
    # break if you find any NaNs
    assert raw_df.isna().sum().sum() == 0, "NaNs detected in {}".format(station_code)
    
    # reconstruct object typed index into DateTimeIndex according to Pandas' law
    raw_df.index = pd.to_datetime(raw_df.index)
    
    # construct an index like it, but guaranteed to have all days in it
    complete_index = pd.DatetimeIndex(start=raw_df.index[0], end=raw_df.index[-1], freq='D')
    
    # create an idx into index that points out the days that should be in raw_df, but are not (~)
    missing_days_idx = ~ complete_index.isin(raw_df.index)
    # create a new, shorter, index containing all the missing days (pull them from complete_index, masked by idx)
    missing_index = complete_index[missing_days_idx]
    n_missing = len(missing_index)
    
    # Only if we are missing days in our index,
    if n_missing > 0:
        print(station_code, "is missing", n_missing, "days: filling up with NaN rows")
        # append missing index to raw_df (creating empty rows), then sort again
        raw_df = raw_df.append(pd.DataFrame(index=missing_index))
        raw_df.sort_index(inplace=True)
    
    # break if the given index differs from the perfect index
    assert raw_df.index[0] == complete_index[0]   , "index still starts funny in {}".format(station_code)
    assert raw_df.index[-1] == complete_index[-1] , "index still ends funny in {}".format(station_code)

    # now construct an hourly index, based on the start/end of the complete index
    hourly_index = pd.DatetimeIndex(start=complete_index[0], periods=24*complete_index.shape[0], freq='H')
    
    # create a clean DataFrame, using the hourly index, with one empty column named 'Q'
    clean_df = pd.DataFrame(index=hourly_index, columns=['Q'])
    clean_df.index.name = "date_time"
    
    start = clean_df.index[0]
    end = clean_df.index[-1]
    start_year = start.year
    end_year = end.year
    
    # dump all numbers from the old, matrix-shaped, raw_df directly into the new, colum-shaped, clean_df
    clean_df['Q'] = raw_df.values.ravel()
    
    # final check to see if the final elements in either df match
    assert clean_df['Q'][-1] == raw_df.iloc[-1, -1] , "final elements do not match in {}".format(station_code)
    
    filename = "Q_{}_hourly_{}-{}.csv".format(station_code, start.year, end.year)
    print(" station {} has Q from year {} until {}".format(station_code, start.year, end.year))
    print(" saving to", filename)
    clean_df.to_csv(filename)
#


#%%



#%%


pd.DataFrame.stack() # creates a df with multilevel index (make sure to rename column headers beforehand)

#%%
raw_df = pd.read_csv("datasets/wallonie/74871002_hor.csv", sep=';\s?', engine='python', index_col=['date'], header=1 )


#%%
raw_df.drop(labels=['Unnamed: 25'], axis=1, inplace=True)


#%%
raw_df.head()


#%%
raw_df.tail()


#%%
assert raw_df.isna().sum().sum() == 0


#%%
raw_df.columns


#%%
print( raw_df.loc["1998-01-01", "heure24"] )


#%%
raw_df.index = pd.to_datetime(raw_df.index)


#%%
given_index = raw_df.index
given_index


#%%
complete_index = pd.DatetimeIndex(start=given_index[0], end=given_index[-1], freq='D')


#%%
complete_index


#%%
missing_days_idx = ~ complete_index.isin(given_index)

missing_index = complete_index[missing_days_idx]


#%%
missing_index


#%%
len(missing_index)


#%%
stuffed_df = raw_df.append(pd.DataFrame(index=missing_index))


#%%
stuffed_df.describe()


#%%
stuffed_df.isna().sum()


#%%
raw_df.index


#%%
stuffed_df.sort_index(inplace=True)


#%%
stuffed_df.tail()


#%%



#%%



#%%
assert given_index[0] == complete_index[0]
assert given_index[-1] == complete_index[-1]


#%%
print(given_index[-1] , complete_index[-1])


#%%
hourly_index = pd.DatetimeIndex(start=complete_index[0], periods=24*complete_index.shape[0], freq='H')


#%%
hourly_index


#%%
clean_df = pd.DataFrame(index=hourly_index, columns=['Q'])


#%%
clean_df['Q'] = raw_df.values.ravel()


#%%
clean_df.describe()


#%%
clean_df.head()


#%%
clean_df.tail()


#%%
assert clean_df['Q'][-1] == raw_df.iloc[-1, -1]

M=64%%time
tabreux_df = pd.DataFrame()

for jour in raw_df.index[:M]:
    for h in range(1,25):
        heure = "heure{}".format(h)
        datetime = "{} {:02d}:00:00".format(jour, h)
        tabreux_df.loc[datetime, "Q"] = raw_df.loc[jour, heure]  # THIS insertion is crazy slow
    #
#

#%%



#%%



#%%



#%%



#%%



#%%



#%%



#%%



