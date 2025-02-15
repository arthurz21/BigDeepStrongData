import pandas as pd
from collections import Counter
import numpy as np
from tqdm import tqdm
import matplotlib.pyplot as plt
import concurrent.futures
import os
from pathlib import Path
import pickle as pkl
import time 


def get_minute_day_ranges(start_date_time, end_date_time):
    # Convert inputs to pandas timestamps
    start_date_time = pd.to_datetime(start_date_time)
    end_date_time = pd.to_datetime(end_date_time)

    # ------------------------------------------
    # 60-minute intervals
    # ------------------------------------------
    # Align the start to the nearest 60-minute boundary (rounding down)
    start_60min = start_date_time.floor('60min')
    # Align the end to the nearest 60-minute boundary (rounding up)
    end_60min = end_date_time.ceil('60min')

    # Create 60-minute interval ranges.
    # We subtract 60 minutes from end_5min because we want full 60-minute blocks.
    five_min_intervals = []
    five_min_starts = pd.date_range(start=start_60min, 
                                    end=end_60min - pd.Timedelta(minutes=5), 
                                    freq='60min')
    for start in five_min_starts:
        # Define the end of the interval as exactly 5 minutes later,
        # subtracting a microsecond so that intervals don't overlap (if needed)
        end = start + pd.Timedelta(minutes=60) - pd.Timedelta(microseconds=1)
        five_min_intervals.append((start, end))

    # ------------------------------------------
    # Daily intervals
    # ------------------------------------------
    # Normalize the start to midnight
    start_day = start_date_time.normalize()
    # Normalize the end to midnight.
    # Note: if end_date_time is not exactly midnight, this represents the beginning
    #       of that day. That day will be included as a full day interval.
    end_day = end_date_time.normalize()

    daily_intervals = []
    # Create a date_range for each day
    day_starts = pd.date_range(start=start_day, end=end_day, freq='D')
    for day in day_starts:
        # Each interval spans the entire day.
        # We set the end to be one day later minus one microsecond.
        day_end = day + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
        daily_intervals.append((day, day_end))

    return five_min_intervals, daily_intervals

def process_single_df(item):
    df_key, df = item
    
    df_customers = sorted(df['customer_id'].unique()) 
    customer_stats_p = pd.DataFrame(index=df_customers)
    start_idx = 0
    curr_idx = 0
    last_idx = None
    
    l = 0
    cust_len = len(df_customers)
    very_start_time = time.time()
    start_time = time.time()
    for customer in df_customers:
        end_flag = False
        while (df.loc[curr_idx, 'customer_id'] == customer):
            # print(df.loc[curr_idx, 'customer_id'])
            curr_idx += 1
            # print(curr_idx)
            if curr_idx == len(df):
                end_flag = True
                break
        last_idx = curr_idx            
        
        if not end_flag:
            customer_df = df.iloc[start_idx:last_idx, :]
        else:
            customer_df = df.iloc[start_idx:, :]

        start_idx = curr_idx
        
        max_credit_minute_trx = 0 
        max_credit_daily_trx = 0
        # max_credit_weekly_trx = 0
        # max_credit_monthly_trx = 0
        max_credit_minute_trx_avg_val = 0
        max_credit_daily_trx_avg_val = 0
        # max_credit_weekly_trx_avg_val = 0
        # max_credit_monthly_trx_avg_val = 0
        
        max_debit_minute_trx = 0
        max_debit_daily_trx = 0
        # max_debit_weekly_trx = 0
        # max_debit_monthly_trx = 0
        max_debit_minute_trx_avg_val = 0
        max_debit_daily_trx_avg_val = 0
        # max_debit_weekly_trx_avg_val = 0
        # max_debit_monthly_trx_avg_val = 0
                
        start_date = df['transaction_datetime'].min()
        end_date = df['transaction_datetime'].max()
        # weekly_ranges, monthly_ranges = get_date_ranges(start_date, end_date)
        minute_ranges, day_ranges = get_minute_day_ranges(start_date, end_date)
 
        
        for i, (start, end) in enumerate(minute_ranges):
            period_df = customer_df[(customer_df['transaction_datetime'] >= start) & (customer_df['transaction_datetime'] <= end)]
            
            credit_period_df = period_df[period_df['debit_credit'] == 'credit']
            debit_period_df = period_df[period_df['debit_credit'] == 'debit']
            
            if len(credit_period_df) > max_credit_minute_trx:
                max_credit_minute_trx = len(credit_period_df)
                max_credit_minute_trx_avg_val = credit_period_df['amount_cad'].mean()    
            if len(debit_period_df) > max_debit_minute_trx:
                max_debit_minute_trx = len(debit_period_df)
                max_debit_minute_trx_avg_val = debit_period_df['amount_cad'].mean()

        customer_stats_p.loc[customer, df_key+'_max_credit_minute_trx'] = max_credit_minute_trx
        customer_stats_p.loc[customer, df_key+'_max_debit_minute_trx_avg_val'] = max_debit_minute_trx_avg_val
        customer_stats_p.loc[customer, df_key+'_max_debit_minute_trx'] = max_debit_minute_trx
        customer_stats_p.loc[customer, df_key+'_max_credit_minute_trx_avg_val'] = max_credit_minute_trx_avg_val
        
        for i, (start, end) in enumerate(day_ranges):
            period_df = customer_df[(customer_df['transaction_datetime'] >= start) & (customer_df['transaction_datetime'] <= end)]
            
            credit_period_df = period_df[period_df['debit_credit'] == 'credit']
            debit_period_df = period_df[period_df['debit_credit'] == 'debit']
            
            if len(credit_period_df) > max_credit_daily_trx:
                max_credit_daily_trx = len(credit_period_df)
                max_credit_daily_trx_avg_val = credit_period_df['amount_cad'].mean()
            
            if len(debit_period_df) > max_debit_daily_trx:
                max_debit_daily_trx = len(debit_period_df)
                max_debit_daily_trx_avg_val = debit_period_df['amount_cad'].mean()

        customer_stats_p.loc[customer, df_key+'_max_credit_daily_trx'] = max_credit_daily_trx
        customer_stats_p.loc[customer, df_key+'_max_credit_daily_trx_avg_val'] = max_credit_daily_trx_avg_val
        customer_stats_p.loc[customer, df_key+'_max_debit_daily_trx'] = max_debit_daily_trx
        customer_stats_p.loc[customer, df_key+'_max_debit_daily_trx_avg_val'] = max_debit_daily_trx_avg_val
        
        l = l+1
        if l%100 == 0:
            curr_time = time.time()
            seconds_remaining = (cust_len - l) * (curr_time - very_start_time) / l
            hours_remaining = seconds_remaining // 3600
            minutes_remaining = (seconds_remaining % 3600) // 60
            seconds_remaining = seconds_remaining % 60
            print(f'{df_key} - Percentage complete: {(l/cust_len)*100}%  - ETA: {int(hours_remaining)}h {int(minutes_remaining)}m {int(seconds_remaining)}s')
            start_time = curr_time
        
    return (df_key, customer_stats_p)


if __name__ == "__main__":
    datapath = Path('processed_synth_dataset/')
    feature_output_dir = Path.mkdir('synth_features', exist_ok=True)
    
    print(datapath.absolute())
    
    frac = 1
    wire = pd.read_csv(datapath/'wire_s.csv', engine="pyarrow").sample(frac = frac)
    ach = pd.read_csv(datapath/'ach_s.csv', engine="pyarrow").sample(frac = frac)
    cheque = pd.read_csv(datapath/'cheque_s.csv', engine="pyarrow").sample(frac = frac)
    card = pd.read_csv(datapath/'card_s.csv', engine="pyarrow").sample(frac = frac)

    dfs = {'card': card, 'wire': wire, 'ach': ach, 'cheque': cheque}
    #Sorting the DFs by date and Time
    for key in dfs.keys():
        dfs[key]['transaction_datetime'] = pd.to_datetime(dfs[key]['transaction_date'].astype(str) + ' ' + dfs[key]['transaction_time'].astype(str))
        dfs[key].sort_values( by = ['customer_id', 'transaction_datetime'] , ascending = [True, True], ignore_index=True, inplace = True)
        
    
    #Collecting all unique customer IDs
    all_customers= list(set(np.concatenate([dfs[trx_type]['customer_id'].unique() for trx_type in dfs.keys()], axis=0)))
    print(len(all_customers))
    
    with concurrent.futures.ProcessPoolExecutor() as executor:
    # dfs.items() produces (df_key, df) tuples.
        results = list((executor.map(process_single_df, dfs.items())))
    
    final_df = pd.concat([value for key, value in results], axis = 1, join='outer')
    # fill all the missing data cells
    final_df.fillna(0, inplace=True)

    # convert the index into a column feature and replace index with just numbers
    final_df = final_df.reset_index().rename(columns={'index': 'customer_id'})
    final_df.to_csv(feature_output_dir/'synth_features_4_5.csv', index = False)