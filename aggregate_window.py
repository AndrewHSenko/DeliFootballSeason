import os
import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'

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