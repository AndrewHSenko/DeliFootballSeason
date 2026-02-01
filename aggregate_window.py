import os
import pandas as pd
from calendar import monthrange

pd.options.mode.chained_assignment = None  # default='warn'

# Combines all window CSV data into a DataFrame after replacing TableID column with the date of that data
window_directory = os.fsencode('/Users/andrew.senkowski/Documents/Raw Football Szn Data')
window_data = pd.DataFrame()

for file in os.listdir(window_directory):
    filename = os.fsdecode(file)
    if filename.endswith("csv") and not filename == 'Football_Data.csv':
        weekend_data = pd.read_csv(os.path.join(str(window_directory)[2:-1], filename), engine='pyarrow', dtype_backend='pyarrow')
        friday = filename.split('-', 1)[0].replace('_', '-')
        month_num = int(friday.split("-", 1)[0])
        year_num = int(filename.split('-', 1)[1].split('_')[2].split(' ')[0])
        friday_day = int(friday.split("-", 1)[1])
        weekend_data['TableID'] = weekend_data['TableID'].astype(str)
        weekend_data['TableID'] = weekend_data['TableID'].replace({
            '1' : friday,
            '2' : f'{month_num}-{friday_day + 1}' if friday_day < monthrange(year_num, month_num)[1] else f'{month_num + 1}-1',
            '3' : f'{month_num}-{friday_day + 2}' if friday_day + 1 < monthrange(year_num, month_num)[1] else f'{month_num + 1}-2',
        })
        window_data = pd.concat([window_data, weekend_data])

window_data.to_parquet('./window.parquet') # Better memory spacewise and faster to read/write than CSV