import os
import pandas as pd

# TO DO #
# - Add try/except error handling
# - Separate into different scripts
# - Create class for day_name_cache usage and helper functions
#########

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

def manage_day_name_cache(day_name_cache, sched, week, col_prefix):
    day_key = f'{week}: {col_prefix}' # Needs week to prevent duplicate column names
    if day_key not in day_name_cache: # Add this key to the cache for more efficient lookup
        temp = sched.loc[sched['Week'] == week].dropna(axis=1, how='all')
        matches = temp.columns[temp.columns.str.startswith(col_prefix)]
        if matches.empty:
            print(f'Day with name: {col_prefix} not found...')
            return
        col_name = matches[0] # The name of the column for the specific day
        day_name_cache[f'{week}: {day_key.split()[-1]}'] = col_name # Stores day name with week to prevent duplicates and the number separately for easier access
    return [day_key, day_name_cache]

def add_call_offs(day_name_cache, sched, week, col_prefix, call_offs):
    cache_result = manage_day_name_cache(day_name_cache, sched, week, col_prefix)
    if cache_result == None:
        print(f'Day with name: {col_prefix} not found...')
        return
    day_name_cache = cache_result[1]
    day_name = day_name_cache[cache_result[0]]
    for call_off in call_offs:
        mask = (sched['Week'] == week) & (sched['Staff'].str.contains(call_off, case=False, regex=False, na=False)) # Retrieves row for specific team member that called off
        sched.loc[mask, day_name] = sched.loc[mask, day_name] + ' CALLED_OFF'
    return True

def update_staff_shift(day_name_cache, sched, week, col_prefix, staff_names, new_times, cut_early=[]):
    cache_result = manage_day_name_cache(day_name_cache, sched, week, col_prefix)
    if cache_result == None:
        print(f'Day with name: {col_prefix} not found...')
        return
    day_name_cache = cache_result[1]
    day_name = day_name_cache[cache_result[0]]
    if len(staff_names) != len(new_times):
        print(f'Updating shifts for {day_name} failed: amount of staff names and time changes do not match')
        return
    for i in range(len(staff_names)): # Implies the staff and times match the same index
        mask = (sched['Week'] == week) & (sched['Staff'].str.contains(staff_names[i], case=False, regex=False, na=False)) # Retrieves row for specific team member that called off
        if not cut_early:
            sched.loc[mask, day_name] = new_times[i] + ' CUT_EARLY'
        else:
            sched.loc[mask, day_name] = new_times[i] + ' CUT_EARLY' if cut_early[i] else new_times[i]
    return True

def add_missing_staff(day_name_cache, sched, week, col_prefix, time, staff_name):
    cache_result = manage_day_name_cache(day_name_cache, sched, week, col_prefix)
    if cache_result == None:
        print(f'Day with name: {col_prefix} not found...')
        return
    day_name_cache = cache_result[1]
    day_name = day_name_cache[cache_result[0]]
    column = sched[day_name].fillna('').astype(str) # Return column as strings with NaN -> ''
    mask = (sched['Week'] == week) & column.str.startswith(time, na=False) # Finds the correct row to add staff name
    sched.loc[mask & sched['Staff'].isna(), 'Staff'] = staff_name
    return True

sched_data = sched_data.sort_values(by=['Week', 'Staff'], ascending=[1, 1]).reset_index(drop=True) # Sorts rows by Week and then by Staff, removing default index

# Fixing 08-25 #

add_missing_staff(day_name_cache, sched_data, '08-25', 'Friday', "11a - 5p", 'Cayenne Leupold')
add_missing_staff(day_name_cache, sched_data, '08-25', 'Friday', "12p - 8p", 'Derek Moreno')
add_missing_staff(day_name_cache, sched_data, '08-25', 'Friday', "7a", 'Kriss McGough')
add_missing_staff(day_name_cache, sched_data, '08-25', 'Friday', "11a - 7p", 'Maureen')
add_missing_staff(day_name_cache, sched_data, '08-25', 'Sunday', "9a", 'Tom White')
update_staff_shift(day_name_cache, sched_data, '08-25', 'Friday', ['Tiff', 'Ron', 'Adam'], ['9a - 5p LEVEL THREE', '6a - 2p LEVEL ONE', '11a - 5p LEVEL THREE'])
update_staff_shift(day_name_cache, sched_data, '08-25', 'Sunday', ['Tiff', 'Dempsey', 'Adam'], ['6a - 1p LEVEL THREE', '7a - 11a LEVEL THREE', '11a - 3p LEVEL THREE'], [True, True, False])
add_call_offs(day_name_cache, sched_data, '08-25', 'Saturday', ['Preston'])


# Fixing 09-01 #
add_missing_staff(day_name_cache, sched_data, '09-01', 'Friday', "12p", 'Cayenne Leupold')
add_missing_staff(day_name_cache, sched_data, '09-01', 'Friday', "11a", 'Maureen')
add_missing_staff(day_name_cache, sched_data, '09-01', 'Saturday', "11a - 7p SUP", 'Derek Moreno')
add_missing_staff(day_name_cache, sched_data, '09-01', 'Sunday', "9a", 'Tom White')
add_call_offs(day_name_cache, sched_data, '09-01', 'Friday', ['Maureen'])
add_call_offs(day_name_cache, sched_data, '09-01', 'Saturday', ['Cayenne', 'Maureen'])
add_call_offs(day_name_cache, sched_data, '09-01', 'Sunday', ['Maureen', 'Albert'])
# NEED TO CHECK TEAM SHEETS FOR UPDATED STAFF SHIFTS #

# Fixing 09-08 #
add_missing_staff(day_name_cache, sched_data, '09-08', 'Friday', "8a", 'Cayenne Leupold')
add_missing_staff(day_name_cache, sched_data, '09-08', 'Friday', "12p - 8p SUP", 'Derek Moreno')
add_missing_staff(day_name_cache, sched_data, '09-08', 'Friday', "11a", 'Maureen')
add_missing_staff(day_name_cache, sched_data, '09-08', 'Sunday', "9a", 'Tom White')
update_staff_shift(day_name_cache, sched_data, '09-08', 'Friday', ['Brandon'], ['9a - 5p LEVEL ONE'], [False])
update_staff_shift(day_name_cache, sched_data, '09-08', 'Saturday', ['Kaye'], ['12p - 7p LEVEL THREE'], [False])
add_call_offs(day_name_cache, sched_data, '09-08', 'Friday', ['Cayenne', 'Maureen', 'Andrew', 'Brigid'])
add_call_offs(day_name_cache, sched_data, '09-08', 'Saturday', ['Nicole'])

# NEED TO FIND 9-14 Team Sheet! #

# Fixing 09-15 #
add_missing_staff(day_name_cache, sched_data, '09-15', 'Friday', "8a", 'Cayenne Leupold')
add_missing_staff(day_name_cache, sched_data, '09-15', 'Friday', "12p - 8p SUP", 'Derek Moreno')
add_missing_staff(day_name_cache, sched_data, '09-15', 'Friday', "11a", 'Maureen')
add_missing_staff(day_name_cache, sched_data, '09-15', 'Sunday', "9a", 'Tom White')
# YOU LEFT OFF HERE #
update_staff_shift(day_name_cache, sched_data, '09-15', 'Friday', ['Brigitte', 'Ron', 'Patricia', 'Cayenne', 'Darrell', 'Ryan', 'Gabriel'], ['6a - 1p LEVEL THREE', '6a - 1p LEVEL ONE', '7a - 4p LEVEL ONE', '8a - 3p ORIENTATION', '6a - 1p LEVEL THREE', '9a - 4p ORIENTATION'])
add_call_offs(day_name_cache, sched_data, '09-15', 'Friday', ['Adam', 'Maureen'])
add_call_offs(day_name_cache, sched_data, '09-15', 'Saturday', ['Adam', 'Brandon'])
add_call_offs(day_name_cache, sched_data, '09-15', 'Sunday', ['Tif', 'Cayenne'])

print(sched_data[sched_data['Week'] == '09-08'].dropna(axis=1, how='all'))


# Fixing 09-22 #
add_missing_staff(day_name_cache, sched_data, '09-22', 'Friday', "8a", 'Cayenne Leupold')
add_missing_staff(day_name_cache, sched_data, '09-22', 'Friday', "12p - 8p SUP", 'Derek Moreno')
add_missing_staff(day_name_cache, sched_data, '09-22', 'Sunday', "9a", 'Tom White')
sched_data.loc[(sched_data['Week'] == '09-22') & (sched_data['Staff'] == 'Brigitte Vincent'), 'Saturday 27'] = '6a - 1p LEVEL THREE'
sched_data.loc[(sched_data['Week'] == '09-22') & (sched_data['Staff'] == 'Tiffany Willson'), 'Saturday 27'] = '6a - 1p LEVEL THREE'
sched_data.loc[(sched_data['Week'] == '09-22') & (sched_data['Staff'] == 'Brigitte Vincent'), 'Saturday 27'] = '6a - 1p LEVEL ONE'
sched_data.loc[(sched_data['Week'] == '09-22') & (sched_data['Staff'] == 'Kris Miller'), 'Saturday 27'] = '6a - 1p SUP'
sched_data.loc[(sched_data['Week'] == '09-22') & (sched_data['Staff'] == 'Aron Gannon'), 'Saturday 27'] = '9a - 7p LEVEL ONE'
sched_data.loc[(sched_data['Week'] == '09-22') & (sched_data['Staff'] == 'Darrell Digesare'), 'Sunday 28'] = '9a - 1p ORIENTATION'
add_call_offs(day_name_cache, sched_data, '09-22', 'Friday', ['Adam'])
add_call_offs(day_name_cache, sched_data, '09-22', 'Saturday', ['Chris'])
add_call_offs(day_name_cache, sched_data, '09-22', 'Sunday', ['Dempsey', 'Chris', 'Aarash'])

# Fixing 09-


level_ones = [

]
level_twos = [

]
level_threes = [
    
]