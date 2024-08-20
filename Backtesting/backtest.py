import pandas as pd
import os
from datetime import datetime, time

def get_time_input():
    time_format = '%H:%M'
    while True:
        time_str = input(f"Enter the time in format {time_format}: ")
        try:
            time_obj = datetime.strptime(time_str, time_format).time()
            return time_obj
        except ValueError:
            print(f"Invalid time format. Please use the format {time_format}.")

def calculate_stats(file_path, input_time, output_file_path):
    df = pd.read_csv(file_path)
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
    df['Time'] = pd.to_datetime(df['Time'], format='%H:%M').dt.time
    
    df_before_user_time = df[df['Time'] <= input_time]
    day_stats = df_before_user_time.groupby('Date').agg(
        DayHigh=('High', 'max'),
        DayLow=('Low', 'min')
    ).reset_index()

    day_stats['CurrentPrice'] = None
    day_stats['ClosingPrice'] = None
    day_stats['InputTime'] = input_time.strftime('%H:%M')
    end_time = time(15, 29)
    # Extract CurrentPrice and ClosingPrice for each date
    for date in day_stats['Date']:
        date_df = df[df['Date'] == date]
        current_price_row = date_df[date_df['Time'] == input_time]
        day_stats.loc[day_stats['Date'] == date, 'CurrentPrice'] = current_price_row['Open'].values[0]

        closing_price_row = date_df[date_df['Time'] == end_time]
        if not closing_price_row.empty:
            day_stats.loc[day_stats['Date'] == date, 'ClosingPrice'] = closing_price_row['Close'].values[0]

    day_stats['isDayHighBreak'] = False
    day_stats['isDayLowBreak'] = False
    day_stats['isItComesToStartingPointForTheHigh'] = False
    day_stats['isItComesToStartingPointForTheLow'] = False

    for index, row in day_stats.iterrows():
        date = row['Date']
        day_high = row['DayHigh']
        day_low = row['DayLow']

        # Filter the data for the period after input_time until end_time
        df_after_input_time = df[(df['Date'] == date) & (df['Time'] > input_time) & (df['Time'] <= end_time)]

        if df_after_input_time['High'].max() >= day_high:
            day_stats.at[index, 'isDayHighBreak'] = True
        if df_after_input_time['Low'].min() <= day_low:
            day_stats.at[index, 'isDayLowBreak'] = True 

        # Check if the high comes back below the DayHigh after breaking it
        if day_stats.at[index, 'isDayHighBreak']:
            if df_after_input_time['High'].lt(day_high).any():
                day_stats.at[index, 'isItComesToStartingPointForTheHigh'] = True
        
        # Check if the low comes back above the DayLow after breaking it
        if day_stats.at[index, 'isDayLowBreak']:
            if df_after_input_time['Low'].gt(day_low).any():
                day_stats.at[index, 'isItComesToStartingPointForTheLow'] = True

    day_stats = day_stats[['Date', 'InputTime', 'DayHigh', 'DayLow', 'isDayHighBreak', 'isDayLowBreak', 'isItComesToStartingPointForTheHigh', 'isItComesToStartingPointForTheLow', 'CurrentPrice', 'ClosingPrice']]
    for column in ['DayHigh', 'DayLow', 'CurrentPrice', 'ClosingPrice']:
        day_stats[column] = day_stats[column].map('{:.2f}'.format)
    day_stats.to_csv(output_file_path, index=False)
    print(f"Processed file and saved results to {output_file_path}")

def process_folder(input_folder, input_time):
    for subdir, _, files in os.walk(input_folder):
        relative_path = os.path.relpath(subdir, input_folder)
        create_folder = os.path.join('Backtesting', relative_path)
        os.makedirs(create_folder, exist_ok=True)

        for file in files:
            if file.endswith('.csv'):
                output_file = os.path.join(create_folder, file)
                file_path = os.path.join(subdir, file)
                print(f"Found CSV file: {file_path}")
                calculate_stats(file_path, input_time, output_file)

if __name__ == "__main__":
    refactored_folder = 'Refactored-Data/'
    input_time = get_time_input()
    process_folder(refactored_folder, input_time)