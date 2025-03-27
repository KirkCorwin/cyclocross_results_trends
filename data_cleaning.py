import os
import pandas as pd 
import importlib
from io import StringIO
import datetime as dt
import numpy as np

def initialize_columns(path, line):
    path='data/'+path
    with open(path, "r") as file:
        lines = file.readlines()

        # add headers first
        return lines[1].lower()

        


        


def clean(path, date, header_path=None, header_line=None):
    CURRENT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
    os.chdir(CURRENT_DIRECTORY)
    path='data/'+path
    with open(path, "r") as file:
        lines = file.readlines()
        
        cleaned_data = []

        # # add headers first
        if header_path is not None:
            cleaned_data.append(initialize_columns(header_path, header_line))
        else: cleaned_data.append(lines[1].lower())

        for line in lines:
            line = line.strip()

            # skip blank lines
            if line == "":
                continue

            # ignore repeated column headers
            headers = ['place', 'beginner', '3-5th', '1-2nd', 'cat', 'clydesdale', 'high', 'male', 'female', 'middle', 'nonbinary', 'single', 'tandem', 'unicycle']
            if line.lower().startswith(tuple(headers)):
                continue

            
            # add the category as the first column to the result rows
            cleaned_data.append(line)


        # join cleaned data into a string and read as a tab-delimited file
        cleaned_str = "\n".join(cleaned_data)
        df = pd.read_csv(StringIO(cleaned_str), sep='\t')


        # 1x cleaned df
        df = df.iloc[1:]
        counter = 0
        columns = df.columns
        new_columns = []
        for column in columns:
            if column.startswith('lap '):
                counter = int(column[-1])
            if column.startswith("Unnamed: "):
                counter += 1
                column = f'lap {counter}'
    
            new_columns.append(column)
        df.columns = new_columns

         # add in date column
        df['date'] = pd.Timestamp(date)

        # make dashes nan
        df.replace('-',np.nan)


        for column in new_columns:
            if column.startswith('lap '):
                # apply conversion to valid time strings, leave nan
                mask = df[column].notna()  # filter out nan rows
                df.loc[mask, column] = pd.to_timedelta('00:' + df.loc[mask, column], errors='coerce').dt.total_seconds()

                # column remains numeric with nan
                df[column] = pd.to_numeric(df[column], errors='coerce')

        # avg lap time col
        lap_columns = df.filter(regex=r'lap (?!.*1)').columns
        df['average_lap_time'] = df[lap_columns].mean(axis=1)
        return df

# need to get average lap time for each racer, lap 2-n are viable, first is too variable

def join_all(file_list, date_list, header_path=None, header_line=None):
    print(f"Initializing main data frame based on: {file_list[0]}")
    main_df = clean(file_list[0], date_list[0], header_path, header_line)
    for file in range(len(file_list)):
        print(f"Appending: {file_list[file]}")
        main_df = pd.concat([main_df, clean(file_list[file], date_list[file])])
    return main_df


events = ['Barnburner at Steilacoom 2023.txt', 'Starcrossed at Marymoor 2023.txt', 'Beach Party At Silver Lake 2023.txt',
'Magnuson Park Cross 2023.txt', 'North 40 at LeMay 2023.txt', 'Woodland Park GP 2023.txt', 'The Beach Party at Silver Lake 2024.txt',
'Starcrossed at Marymoor 2024.txt', 'Barnburner at Steilacoom 2024.txt', 'Magnuson Park Cross 2024.txt', 'North 40 - LeMay 2024.txt',
'Woodland Park Gran Prix 2024.txt']
dates = ['Sep 10, 2023', 'Sep 23, 2023', 'Oct 8, 2023', 'Oct 22, 2023', 'Nov 5, 2023', 'Nov 19, 2023', 'Sep 8, 2024', 'Sep 21, 2024', 'Oct 6, 2024', 'Oct 20, 2024', 'Nov 3, 2024', 'Nov 17, 2024']

df = join_all(events, dates, events[-1], 331)

df.columns