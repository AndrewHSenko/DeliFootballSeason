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

def manage_day_name_cache(day_name_cache, sched, week, day):
    day_key = f'{week}: {day}' # Needs week to prevent duplicate column names
    if day_key not in day_name_cache: # Add this key to the cache for more efficient lookup
        temp = sched.loc[sched['Week'] == week].dropna(axis=1, how='all')
        matches = temp.columns[temp.columns.str.startswith(day)]
        if matches.empty:
            print(f'Day with name: {day} not found...')
            return
        col_name = matches[0] # The name of the column for the specific day
        day_name_cache[f'{week}: {day_key.split()[-1]}'] = col_name # Stores day name with week to prevent duplicates and the number separately for easier access
    return [day_key, day_name_cache]

# day_name_cache : cache of column names for quicker access
# sched : the schedule dataframe
# week : desired week
# day : day of the week (aka prefix of column)
# staff_changes : dictionary of changes for staff
def update_staff_shift(day_name_cache, sched, week, day, staff_changes):
    cache_result = manage_day_name_cache(day_name_cache, sched, week, day)
    if cache_result == None:
        print(f'Day with name: {day} not found...')
        return
    day_name_cache = cache_result[1]
    day_name = day_name_cache[cache_result[0]]
    # staff_changes is staff_name : {call_off : bool, cut_early : str (new time), stayed_late : str (new time), new_entry : str (new time)}
    for name, changes in staff_changes.items():
        mask = (sched['Week'] == week) & (sched['Staff'].str.contains(name, case=False, regex=False, na=False)) # Retrieves row for specific team member that called off
        if changes['call_off']:
            sched.loc[mask, day_name] += ' CALLED_OFF'
        elif changes['cut_early']:
            sched.loc[mask, day_name] = changes['cut_early'] + ' CUT_EARLY'
        elif changes['stayed_late']:
            sched.loc[mask, day_name] = changes['stayed_late'] + ' STAYED_LATE'
        elif changes['new_entry']:
            sched.loc[mask, day_name] = changes['new_entry'] + ' UNSCHEDULED'
    return True

def add_missing_staff(day_name_cache, sched, week, day, time, staff_name):
    cache_result = manage_day_name_cache(day_name_cache, sched, week, day)
    if cache_result == None:
        print(f'Day with name: {day} not found...')
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
update_staff_shift(day_name_cache, sched_data, '08-25', 'Friday', {
    'Tiff' : {'cut_early' : '9a - 5p LEVEL THREE'},
    'Ron' : {'cut_early' : '6a - 2p LEVEL ONE'},
    'Adam' : {'cut_early' : '11a - 5p LEVEL THREE'}
})

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
update_staff_shift(day_name_cache, sched_data, '09-15', 'Friday', ['Brigitte', 'Ron', 'Patricia', 'Cayenne', 'Darrell', 'Ryan', 'Gabriel'], ['6a - 1p LEVEL THREE', '6a - 1p LEVEL ONE', '7a - 4p LEVEL ONE', '8a - 3p ORIENTATION', '6a - 1p LEVEL THREE', '9a - 4p ORIENTATION', '10a - 3:30p ORIENTATION'])
add_call_offs(day_name_cache, sched_data, '09-15', 'Friday', ['Adam', 'Maureen'])
add_call_offs(day_name_cache, sched_data, '09-15', 'Saturday', ['Adam', 'Brandon'])
add_call_offs(day_name_cache, sched_data, '09-15', 'Sunday', ['Tif', 'Cayenne'])


# Fixing 09-22 #
add_missing_staff(day_name_cache, sched_data, '09-22', 'Friday', "8a", 'Cayenne Leupold')
add_missing_staff(day_name_cache, sched_data, '09-22', 'Friday', "12p - 8p SUP", 'Derek Moreno')
add_missing_staff(day_name_cache, sched_data, '09-22', 'Sunday', "9a", 'Tom White')
update_staff_shift(day_name_cache, sched_data, '09-22', 'Saturday', ['Brigitte', 'Tiffany', 'Kris', 'Aron'], ['6a - 1p LEVEL THREE', '6a - 1p LEVEL ONE', '6a - 1p SUP', '9a - 7p LEVEL ONE'], [True, True, True, False])
update_staff_shift(day_name_cache, sched_data, '09-22', 'Sunday', ['Ox'], ['9a - 1p ORIENTATION'])
add_call_offs(day_name_cache, sched_data, '09-22', 'Friday', ['Adam'])
add_call_offs(day_name_cache, sched_data, '09-22', 'Saturday', ['Chris'])
add_call_offs(day_name_cache, sched_data, '09-22', 'Sunday', ['Dempsey', 'Chris', 'Aarash'])


# Fixing 09-29 #
add_missing_staff(day_name_cache, sched_data, '09-29', 'Friday', '6a', 'Cayenne Leupold')
add_missing_staff(day_name_cache, sched_data, '09-29', 'Friday', '12p', 'Derek Moreno')
update_staff_shift(day_name_cache, sched_data, '09-29', 'Sunday', ['Brigid'], ['12p - 1:30p LEVEL TWO'])

# Fixing 10-06 #
add_missing_staff(day_name_cache, sched_data, '10-06', 'Friday', "6a", 'Cayenne Leupold')
update_staff_shift(day_name_cache, sched_data, '10-06', 'Friday', ['Adam'], ['9a - 4p LEVEL THREE'])
update_staff_shift(day_name_cache, sched_data, '10-06', 'Saturday', ['Brigitte'], ['6a - 3:30p LEVEL THREE'], [False])
update_staff_shift(day_name_cache, sched_data, '10-06', 'Sunday', ['Kris'], ['11a - 4:30p LEVEL THREE'])
add_call_offs(day_name_cache, sched_data, '10-06', 'Friday', ['Zach', 'John'])
add_call_offs(day_name_cache, sched_data, '10-06', 'Saturday', ['John'])
add_call_offs(day_name_cache, sched_data, '10-06', 'Sunday', ['Brandon', 'John'])

# Fixing 10-13 #
sched_data.loc[sched_data['Week'] == '10-13'] = sched_data[sched_data['Week'] == '10-13'].drop_duplicates(keep='first') # From bad CSV file
sched_data.drop(sched_data[sched_data['Staff'] == 'Annotations'].index, inplace=True)
sched_data.drop(sched_data[sched_data['Staff'].str.contains('Washington')].index, inplace=True)
add_missing_staff(day_name_cache, sched_data, '10-13', 'Friday', "6a", 'Cayenne Leupold')
add_missing_staff(day_name_cache, sched_data, '10-13', 'Friday', "12p - 8p SUP", 'Derek Moreno')
add_missing_staff(day_name_cache, sched_data, '10-13', 'Saturday', "12p", 'John Grundhofer')
update_staff_shift(day_name_cache, sched_data, '10-13', 'Friday', ['Ryan'], ['9a - 5p LEVEL THREE'], [False])
add_call_offs(day_name_cache, sched_data, '10-13', 'Friday', ['Cayenne', 'Patricia'])
add_call_offs(day_name_cache, sched_data, '10-13', 'Saturday', ['John'])

# Fixing 10-20 #
sched_data.loc[sched_data['Week'] == '10-20'] = sched_data[sched_data['Week'] == '10-20'].drop_duplicates(keep='first') # From bad CSV file
sched_data.drop(sched_data[sched_data['Staff'].str.contains('Michigan')].index, inplace=True)
sched_data.drop(sched_data[sched_data['Saturday 25 (1)'] == 'HOLIDAY ALL DAY'].index, inplace = True)
add_missing_staff(day_name_cache, sched_data, '10-20', 'Friday', "7a", 'Cayenne Leupold')
add_missing_staff(day_name_cache, sched_data, '10-20', 'Friday', "12p - 8p SUP", 'Derek Moreno')
add_missing_staff(day_name_cache, sched_data, '10-20', 'Saturday', "12p", 'John Grundhofer')
update_staff_shift(day_name_cache, sched_data, '10-20', 'Sunday', ['Albert', 'Zach', 'Cayenne'], ['9a - 5p LEVEL THREE', '11a - 5:30p LEVEL THREE', '12p - 7p ORIENTATION'])
add_call_offs(day_name_cache, sched_data, '10-20', 'Saturday', ['Cayenne', 'Brandon'])
add_call_offs(day_name_cache, sched_data, '10-20', 'Sunday', ['Brandon', 'Ron'])

# Fixing 10-27 #

add_missing_staff(day_name_cache, sched_data, '10-27', 'Friday', "7a", 'Cayenne Leupold')
add_missing_staff(day_name_cache, sched_data, '10-27', 'Friday', "12p", 'Derek Moreno')
update_staff_shift(day_name_cache, sched_data, '10-27', 'Saturday', ['Brigitte'], ['11a - 5p LEVEL THREE'], [False])
update_staff_shift(day_name_cache, sched_data, '10-27', 'Sunday', ['Adam'], ['10a - 4p LEVEL THREE'], [False])
add_call_offs(day_name_cache, sched_data, '10-27', 'Friday', ['Zach'])
add_call_offs(day_name_cache, sched_data, '10-27', 'Saturday', ['Darrell', 'Zach'])
add_call_offs(day_name_cache, sched_data, '10-27', 'Sunday', ['Darrell', 'Zach', 'Chris'])

# Fixing 11-03 #

sched_data.loc[sched_data['Week'] == '11-03'] = sched_data[sched_data['Week'] == '11-03'].drop_duplicates(keep='first') # From bad CSV file
add_missing_staff(day_name_cache, sched_data, '11-03', 'Friday', "7a", 'Cayenne Leupold')
add_missing_staff(day_name_cache, sched_data, '11-03', 'Friday', "12p", 'Derek Moreno')
update_staff_shift(day_name_cache, sched_data, '11-03', 'Sunday', [
    'Dempsey', 'Ron', 'Brigitte', 'Tif', 'Jun', 'Darrell', 'Rowan', 'Justin', 'Albert', 'Zach', 'Kris', 'Ryan'
    ],
    [
    '6a - 12:30p LEVEL THREE', '6a - 11:30p LEVEL ONE', '6a = 12:30p LEVEL THREE', '6a - 7:30a LEVEL THREE', '8a - 2p LEVEL ONE', '9a - 3:30p LEVEL ONE', '9a - 4:30p LEVEL THREE', '10a - 3:30p ORIENTATION', '11a - 5:30p LEVEL THREE', '11a - 5p LEVEL THREE', '11a - 4:30p LEVEL THREE', '12p - 7p LEVEL THREE'
    ])
update_staff_shift(day_name_cache, sched_data, '11-03', 'Sunday', ['Adam'], ['10a - 1:30p LEVEL THREE'])
add_call_offs(day_name_cache, sched_data, '11-03', 'Friday', ['Cayenne'])
add_call_offs(day_name_cache, sched_data, '11-03', 'Saturday', ['Rowan'])
add_call_offs(day_name_cache, sched_data, '11-03', 'Sunday', ['Brigid'])

print(sched_data[sched_data['Week'] == '11-10'].dropna(axis=1, how='all'))
# Fixing 11-10 #


level_ones = [

]
level_twos = [

]
level_threes = [
    
]