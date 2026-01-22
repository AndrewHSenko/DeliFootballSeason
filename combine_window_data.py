import os
import pandas as pd

directory = os.fsencode('/Users/andrew.senkowski/Documents/Raw Football Szn Data')
big_data = pd.DataFrame()

# Combines all window CSV data into a DataFrame after replacing TableID column with the date of that data
for file in os.listdir(directory):
    filename = os.fsdecode(file)
    if filename.endswith("csv"):
        friday = filename.split('-', 1)[0]
        weekend_data = pd.read_csv(os.path.join(str(directory)[2:-1], filename))
        weekend_data['TableID'] = weekend_data['TableID'].replace({
            1 : friday,
            2 : f'{friday.split("_", 1)[0]}_{int(friday.split("_", 1)[1]) + 1}',
            3 : f'{friday.split("_", 1)[0]}_{int(friday.split("_", 1)[1]) + 2}'
        })
        big_data = pd.concat([big_data, weekend_data])

# Merges shift scheduling into window DataFrame
level_ones = [

]
level_twos = [

]
level_threes = [
    
]