import os
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

# TO DO #
# - Add try/except error handling
# - Create class for day_name_cache usage and helper functions
#########

# Merges shift scheduling into window DataFrame
sched_directory = os.fsencode('/Users/andrew.senkowski/Downloads/footballsznschedule')
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

level_ones = [
    'Maureen', 'Aarash Baktash', 'Darrell digesare', 'Tom White', 'Brian Lee', 'Gabriel Peoples', 'aron gannon', 'Kriss McGough', 'john Grundhofer', 'Jun Won', 'Ronald Maxwell', 'justin briggs', 'Cayenne Leupold', 'Maureen', 'lacy Zeallear'
]
level_twos = [
    'Chris Campbell', 'patricia lewis', 'Brandon Laird', 'Brigitte Vincent', 'Nicole Young', 'Lucas schmidt'
]
level_threes = [
    'Andrew Senkowski', 'Brigid Gunnerson', 'Kris Miller', 'Rowan Johnson', 'Tiffany Willson', 'Derek Moreno', 'Adam Dale', 'Ryan Nutter', 'preston mann', 'Zach Martin', 'Kaye Li', 'Albert Garrow', 'Brendan Dempsey', 'Savina Osann', 'Nevada Moening'
]

def get_level(staff_name):
    if pd.isna(staff_name):
        return None
    if staff_name in level_ones:
        return 1
    elif staff_name in level_twos:
        return 2
    elif staff_name in level_threes:
        return 3
    else:
        return None

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
        mask = (sched['Week'] == week) & (sched['Staff'].str.contains(name, case=True, regex=False, na=False)) # Retrieves row for specific team member that called off
        if 'called_off' in changes and changes['called_off']:
            sched.loc[mask, day_name] += ' CALLED_OFF'
        elif 'cut_early' in changes:
            sched.loc[mask, day_name] = changes['cut_early'] + ' CUT_EARLY'
        elif 'stayed_late' in changes:
            sched.loc[mask, day_name] = changes['stayed_late'] + ' STAYED_LATE'
        elif 'new_entry' in changes:
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
    'Tif' : {'cut_early' : '9a - 5p LEVEL THREE'},
    'Ron' : {'cut_early' : '6a - 2p LEVEL ONE'},
    'Adam' : {'cut_early' : '11a - 5p LEVEL THREE'}
})
update_staff_shift(day_name_cache, sched_data, '08-25', 'Saturday', {
    'preston' : {'called_off' : True}
})
update_staff_shift(day_name_cache, sched_data, '08-25', 'Sunday', {
    'Tif' : {'cut_early' : '6a - 1p LEVEL THREE'},
    'Dempsey' : {'cut_early' : '7a - 11a LEVEL THREE'},
    'Adam' : {'new_entry' : '11a - 3p LEVEL THREE'}
})

# Fixing 09-01 #
add_missing_staff(day_name_cache, sched_data, '09-01', 'Friday', "12p", 'Cayenne Leupold')
add_missing_staff(day_name_cache, sched_data, '09-01', 'Friday', "11a", 'Maureen')
add_missing_staff(day_name_cache, sched_data, '09-01', 'Saturday', "11a - 7p SUP", 'Derek Moreno')
add_missing_staff(day_name_cache, sched_data, '09-01', 'Sunday', "9a", 'Tom White')
update_staff_shift(day_name_cache, sched_data, '09-01', 'Friday', {
    'Maureen' : {'called_off' : True}
})
update_staff_shift(day_name_cache, sched_data, '09-01', 'Saturday', {
    'Cayenne' : {'called_off' : True},
    'Maureen' : {'called_off' : True}
})
update_staff_shift(day_name_cache, sched_data, '09-01', 'Sunday', {
    'Maureen' : {'called_off' : True},
    'Albert' : {'called_off' : True}
})

# Fixing 09-08 #
add_missing_staff(day_name_cache, sched_data, '09-08', 'Friday', "8a", 'Cayenne Leupold')
add_missing_staff(day_name_cache, sched_data, '09-08', 'Friday', "12p - 8p SUP", 'Derek Moreno')
add_missing_staff(day_name_cache, sched_data, '09-08', 'Friday', "11a", 'Maureen')
add_missing_staff(day_name_cache, sched_data, '09-08', 'Sunday', "9a", 'Tom White')
update_staff_shift(day_name_cache, sched_data, '09-08', 'Friday', {
    'Brandon' : {'new_entry' : '9a - 5p LEVEL ONE'},
    'Cayenne' : {'called_off' : True},
    'Maureen' : {'called_off' : True},
    'Andrew' : {'called_off' : True},
    'Brigid' : {'called_off' : True},
})
update_staff_shift(day_name_cache, sched_data, '09-08', 'Saturday', {
    'Kaye' : {'new_entry' : '12p - 7p LEVEL THREE'},
    'Nicole' : {'called_off' : True}
})

# Fixing 09-15 #
add_missing_staff(day_name_cache, sched_data, '09-15', 'Friday', "8a", 'Cayenne Leupold')
add_missing_staff(day_name_cache, sched_data, '09-15', 'Friday', "12p - 8p SUP", 'Derek Moreno')
add_missing_staff(day_name_cache, sched_data, '09-15', 'Friday', "11a", 'Maureen')
add_missing_staff(day_name_cache, sched_data, '09-15', 'Sunday', "9a", 'Tom White')
update_staff_shift(day_name_cache, sched_data, '09-15', 'Friday', {
    'Brigitte' : {'cut_early' : '6a - 1p LEVEL THREE'},
    'Ron' : {'cut_early' : '6a - 1p LEVEL THREE'},
    'patricia' : {'cut_early' : '7a - 4p LEVEL ONE'},
    'Cayenne' : {'cut_early' : '8a - 3p ORIENTATION'},
    'Darrell' : {'cut_early' : '6a - 1p LEVEL THREE'},
    'Ryan' : {'cut_early' : '9a - 4p ORIENTATION'},
    'Gabriel' : {'cut_early' : '10a - 3:30p ORIENTATION'},
    'Adam' : {'called_off' : True},
    'Maureen' : {'called_off' : True}
})
update_staff_shift(day_name_cache, sched_data, '09-15', 'Saturday', {
    'Adam' : {'called_off' : True},
    'Brandon' : {'called_off' : True}
})
update_staff_shift(day_name_cache, sched_data, '09-15', 'Sunday', {
    'Tif' : {'called_off' : True},
    'Cayenne' : {'called_off' : True}
})

# Fixing 09-22 #
add_missing_staff(day_name_cache, sched_data, '09-22', 'Friday', "8a", 'Cayenne Leupold')
add_missing_staff(day_name_cache, sched_data, '09-22', 'Friday', "12p - 8p SUP", 'Derek Moreno')
add_missing_staff(day_name_cache, sched_data, '09-22', 'Sunday', "9a", 'Tom White')
update_staff_shift(day_name_cache, sched_data, '09-22', 'Friday', {
    'Adam' : {'called_off' : True}
})
update_staff_shift(day_name_cache, sched_data, '09-22', 'Saturday', {
    'Brigitte' : {'cut_early' : '6a - 1p LEVEL THREE'},
    'Tif' : {'cut_early' : '6a - 1p LEVEL ONE'},
    'Kris' : {'cut_early' : '6a - 1p SUP'},
    'aron' : {'new_entry' : '9a - 7p LEVEL ONE'},
    'Chris' : {'called_off' : True}
})
update_staff_shift(day_name_cache, sched_data, '09-22', 'Sunday', {
    'Kaye' : {'cut_early' : '9a - 1p ORIENTATION'},
    'Dempsey' : {'called_off' : True},
    'Chris' : {'called_off' : True},
    'Aarash' : {'called_off' : True}
})

# Fixing 09-29 #
add_missing_staff(day_name_cache, sched_data, '09-29', 'Friday', '6a', 'Cayenne Leupold')
add_missing_staff(day_name_cache, sched_data, '09-29', 'Friday', '12p', 'Derek Moreno')
update_staff_shift(day_name_cache, sched_data, '09-29', 'Sunday', {
    'Brigid' : {'cut_early' : '12p - 1:30p LEVEL TWO'}
})

# Fixing 10-06 #
add_missing_staff(day_name_cache, sched_data, '10-06', 'Friday', "6a", 'Cayenne Leupold')
update_staff_shift(day_name_cache, sched_data, '10-06', 'Friday', {
    'Adam' : {'cut_early' : '9a - 4p LEVEL THREE'},
    'Zach' : {'called_off' : True},
    'john' : {'called_off' : True}
})
update_staff_shift(day_name_cache, sched_data, '10-06', 'Saturday', {
    'Brigitte' : {'stayed_late' : '6a - 3:30p LEVEL THREE'},
    'john' : {'called_off' : True}
})
update_staff_shift(day_name_cache, sched_data, '10-06', 'Sunday', {
    'Tif' : {'cut_early' : '11a - 4:30p LEVEL THREE'},
    'Brandon' : {'called_off' : True},
    'john' : {'called_off' : True}
})

# Fixing 10-13 #
sched_data.loc[sched_data['Week'] == '10-13'] = sched_data[sched_data['Week'] == '10-13'].drop_duplicates(keep='first') # From bad CSV file
sched_data.drop(sched_data[sched_data['Staff'] == 'Annotations'].index, inplace=True)
sched_data.drop(sched_data[sched_data['Staff'].str.contains('Washington')].index, inplace=True)
add_missing_staff(day_name_cache, sched_data, '10-13', 'Friday', "6a", 'Cayenne Leupold')
add_missing_staff(day_name_cache, sched_data, '10-13', 'Friday', "12p - 8p SUP", 'Derek Moreno')
add_missing_staff(day_name_cache, sched_data, '10-13', 'Saturday', "12p", 'john Grundhofer')
update_staff_shift(day_name_cache, sched_data, '10-13', 'Friday', {
    'Ryan' : {'new_entry' : '9a - 5p LEVEL THREE'},
    'Cayenne' : {'called_off' : True},
    'patricia' : {'called_off' : True}
})
update_staff_shift(day_name_cache, sched_data, '10-13', 'Saturday', {
    'john' : {'called_off' : True}
})

# Fixing 10-20 #
sched_data.loc[sched_data['Week'] == '10-20'] = sched_data[sched_data['Week'] == '10-20'].drop_duplicates(keep='first') # From bad CSV file
sched_data.drop(sched_data[sched_data['Staff'].str.contains('Michigan')].index, inplace=True)
sched_data.drop(sched_data[sched_data['Saturday 25 (1)'] == 'HOLIDAY ALL DAY'].index, inplace = True)
add_missing_staff(day_name_cache, sched_data, '10-20', 'Friday', "7a", 'Cayenne Leupold')
add_missing_staff(day_name_cache, sched_data, '10-20', 'Friday', "12p - 8p SUP", 'Derek Moreno')
add_missing_staff(day_name_cache, sched_data, '10-20', 'Saturday', "12p", 'john Grundhofer')
update_staff_shift(day_name_cache, sched_data, '10-20', 'Saturday', {
    'Cayenne' : {'called_off' : True},
    'Brandon' : {'called_off' : True}
})
update_staff_shift(day_name_cache, sched_data, '10-20', 'Sunday', {
    'Albert' : {'cut_early' : '9a - 5p LEVEL THREE'},
    'Zach' : {'cut_early' : '11a - 5:30p LEVEL THREE'},
    'Cayenne' : {'cut_early' : '12a - 7p LEVEL THREE'},
    'Brandon' : {'called_off' : True},
    'Ron' : {'called_off' : True}
})

# Fixing 10-27 #
add_missing_staff(day_name_cache, sched_data, '10-27', 'Friday', "7a", 'Cayenne Leupold')
add_missing_staff(day_name_cache, sched_data, '10-27', 'Friday', "12p", 'Derek Moreno')
update_staff_shift(day_name_cache, sched_data, '10-27', 'Friday', {
    'Zach' : {'called_off' : True}
})
update_staff_shift(day_name_cache, sched_data, '10-27', 'Saturday', {
    'Brigitte' : {'new_entry' : '11a - 5p LEVEL THREE'},
    'Darrell' : {'called_off' : True},
    'Zach' : {'called_off' : True}
})
update_staff_shift(day_name_cache, sched_data, '10-27', 'Sunday', {
    'Adam' : {'new_entry' : '10a - 4p LEVEL THREE'},
    'Darrell' : {'called_off' : True},
    'Zach' : {'called_off' : True},
    'Chris' : {'called_off' : True}
})

# Fixing 11-03 #
sched_data.loc[sched_data['Week'] == '11-03'] = sched_data[sched_data['Week'] == '11-03'].drop_duplicates(keep='first') # From bad CSV file
add_missing_staff(day_name_cache, sched_data, '11-03', 'Friday', "7a", 'Cayenne Leupold')
add_missing_staff(day_name_cache, sched_data, '11-03', 'Friday', "12p", 'Derek Moreno')
update_staff_shift(day_name_cache, sched_data, '11-03', 'Friday', {
    'Cayenne' : {'called_off' : True}
})
update_staff_shift(day_name_cache, sched_data, '11-03', 'Saturday', {
    'Brigitte' : {'new_entry' : '11a - 5p LEVEL THREE'},
    'Rowan' : {'called_off' : True}
})
update_staff_shift(day_name_cache, sched_data, '11-03', 'Sunday', {
    'Dempsey' : {'cut_early' : '6a - 12:30p LEVEL THREE'},
    'Ron' : {'cut_early' : '6a - 11:30p LEVEL ONE'},
    'Brigitte' : {'cut_early' : '6a = 12:30p LEVEL THREE'},
    'Tif' : {'cut_early' : '6a - 7:30a LEVEL THREE'},
    'Jun' : {'cut_early' : '8a - 2p LEVEL ONE'},
    'Darrell' : {'cut_early' : '9a - 3:30p LEVEL ONE'},
    'Rowan' : {'cut_early' : '9a - 4:30p LEVEL THREE'},
    'justin' : {'cut_early' : '10a - 3:30p ORIENTATION'},
    'Albert' : {'cut_early' : '11a - 5:30p LEVEL THREE'},
    'Zach' : {'cut_early' : '11a - 5p LEVEL THREE'},
    'Kris' : {'cut_early' : '11a - 4:30p LEVEL THREE'},
    'Ryan' : {'cut_early' : '12p - 7p LEVEL THREE'},
    'Brigid' : {'called_off' : True}
})

# Fixing 11-10 #
add_missing_staff(day_name_cache, sched_data, '11-10', 'Friday', "8a", 'Cayenne Leupold')
add_missing_staff(day_name_cache, sched_data, '11-10', 'Friday', "12p - 8p SUP", 'Derek Moreno')
update_staff_shift(day_name_cache, sched_data, '11-10', 'Saturday', {
    'Ryan' : {'cut_early' : '10a - 7p LEVEL THREE'},
    'Cayenne' : {'called_off' : True},
    'Kaye' : {'called_off' : True}
})
update_staff_shift(day_name_cache, sched_data, '10-06', 'Sunday', {
    'Dempsey' : {'cut_early' : '6a - 1p LEVEL THREE'},
    'Rowan' : {'cut_early' : '9a - 4:30p LEVEL THREE'},
    'Albert' : {'cut_early' : '11a - 5p LEVEL THREE'},
    'Brandon' : {'cut_early' : '11a - 6p LEVEL THREE'}
})

# Fixing 11-17 #
add_missing_staff(day_name_cache, sched_data, '11-17', 'Friday', '12p - 8p SUP', 'Derek Moreno')

# Adding Level column #
sched_data['Level'] = sched_data['Staff'].apply(get_level)

sched_data.to_parquet() # Better memory spacewise and faster to read/write than CSV