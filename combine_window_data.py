import os
import pandas as pd

# Combines all window CSV data into a DataFrame after replacing TableID column with the date of that data
# window_directory = os.fsencode('/Users/andrew.senkowski/Documents/Raw Football Szn Data')
# wind_data = pd.DataFrame()

# for file in os.listdir(window_directory):
#     filename = os.fsdecode(file)
#     if filename.endswith("csv"):
#         friday = filename.split('-', 1)[0]
#         weekend_data = pd.read_csv(os.path.join(str(window_directory)[2:-1], filename), engine='pyarrow', dtype_backend='pyarrow')
#         weekend_data['TableID'] = weekend_data['TableID'].replace({
#             1 : friday,
#             2 : f'{friday.split("_", 1)[0]}_{int(friday.split("_", 1)[1]) + 1}',
#             3 : f'{friday.split("_", 1)[0]}_{int(friday.split("_", 1)[1]) + 2}'
#         })
#         wind_data = pd.concat([wind_data, weekend_data])

# Merges shift scheduling into window DataFrame
sched_directory = os.fsencode('/Users/andrewsenkowski/Downloads/footballsznschedule')
sched_data = pd.DataFrame()

for file in os.listdir(sched_directory):
    filename = os.fsdecode(file)
    if filename.endswith("csv"):
        week_sched_raw = pd.read_csv(os.path.join(str(sched_directory)[2:-1], filename), engine='pyarrow', dtype_backend='pyarrow')
        week_sched = week_sched_raw.filter(regex="^Friday|Saturday|Sunday|Staff")
        week_sched.replace(r'\n', ' ', regex=True, inplace=True) # Cleans out newline characters
        filename_split = filename.split('-')
        week_sched['Week'] = f'{filename_split[2]}-{filename_split[3][:-4]}' # Identifies the week of the schedule
        sched_data = pd.concat([week_sched, sched_data])

day_name_cache = {}

def add_missing_staff(day_name_cache, sched, week, col_prefix, time, name):
    day_key = f'{week}: {col_prefix}' # Needs week to prevent duplicate column names
    if day_key in day_name_cache: # Implementation of cache once a column is found
        day_name = day_name_cache[day_key]
    else: # Find the desired column
        temp = sched.loc[sched['Week'] == week].dropna(axis=1, how='all')
        matches = temp.columns[temp.columns.str.startswith(col_prefix)]
        if matches.empty:
            print(f'Day with name: {col_prefix} not found...')
            return
        col_name = matches[0] # The name of the column for the specific day
        day_name_cache[f'{week}: {day_name.split()[-2]}'] = col_name # Stores day name with week to prevent duplicates and the number separately for easier access
    column = sched[day_name].fillna('').astype(str) # Return column as strings with NaN -> ''
    mask = (sched['Week'] == week) & column.str.startswith(time, na=False)
    sched.loc[mask & sched['Staff'].isna(), 'Staff'] = name
    return day_name_cache

sched_data = sched_data.sort_values(by=['Week', 'Staff'], ascending=[1, 1]).reset_index(drop=True) # Sorts rows by Week and then by Staff, removing default index
sched_data[sched_data['Week'] == '08-25'].dropna(axis=1, how='all')
day_name_cache = add_missing_staff(day_name_cache, sched_data, '08-25', 'Friday', "11a - 5p", 'Cayenne Leupold')
day_name_cache = add_missing_staff(day_name_cache, sched_data, '08-25', 'Friday', "12p - 8p", 'John Grundhofer')
print(sched_data[sched_data['Week'] == '08-25'].dropna(axis=1, how='all'))


level_ones = [

]
level_twos = [

]
level_threes = [
    
]