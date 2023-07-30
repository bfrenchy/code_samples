"""
This file contains the functions for calculating the estimated metrics in local currency for each company and subcategory in the master table.

Functions
---------
metric_calculator(basic_data, master_table=master_table)
    Calculates the estimated metrics in local currency for each company and subcategory in the master table.

calculate_metric(master_table=master_table)
    Exports basic_data and then applies the metric_calculator function to it.

Module imports
--------------
utils.config.master_table: contains the master table from the database
utils.functions.create_date_range: creates a date range from the start to the end of the forecast
utils.database_export_funcs.basic_data_export: exports the basic_data table from the database
"""

import pandas as pd
import numpy as np

from ..utils.config import master_table
from ..utils.functions import convert_period_to_datetime, create_date_range
from ..utils.database_export_funcs import basic_data_export

def metric_calculator(basic_data, master_table=master_table):
    """
    Calculates the estimated metrics in local currency for each company and subcategory in the master table.

    Parameters
    ----------
    basic_data : pandas.DataFrame
        The basic_data table from the database.
    master_table : pandas.DataFrame
        The master_table from the database.

    Returns
    -------
    pandas.DataFrame
        A dataframe with the following columns:
            - date
            - time
            - region
            - territory
            - currency
            - economy_metric
            - exchange_rate
            - company
            - subcategory
            - year
            - period
    """
    date_range, time_range = create_date_range()
    print(f"Calculating estimated metrics in local currency from {time_range[0]} to {time_range[-1]}.")
    
    # Create a new column in master_table that combines the service_type and channel_type columns
    print("Creating subcategory column in master_table DataFrame...")
    new_col = 'subcategory'
    master_table[new_col] = np.where(
        pd.isna(master_table['channel_type']),
        master_table['service_type'],
        master_table['service_type'] + ' - ' + master_table['channel_type'])
    
    # Prepare the DataFrame for metric calculations
    print("Preparing DataFrame for metric calculations...")
    df = pd.DataFrame(index=[basic_data['date'], 
                             basic_data['time'], 
                             basic_data['region'], 
                             basic_data['territory'], 
                             basic_data['currency'],
                             basic_data['economy_metric'],
                             basic_data['exchange_rate']],
                      columns=[master_table['company'], master_table['subcategory']])
    df.reset_index(inplace=True)

    col_list = list(df.columns)
    i = 7

    # Calculate the metric for each company/subcategory
    print("Calculating metrics...")
    while i < len(col_list):
        company = col_list[i][0]
        subcategory = col_list[i][1]
        
        row = master_table[(master_table['company'] == company) & (master_table['subcategory'] == subcategory)]
        
        if not row.empty:
            index_time = row['index_time'].values[0]
            index_date = convert_period_to_datetime(index_time)
            index_territory = row['base_territory'].values[0]
            index_currency = row['base_currency'].values[0]
            index_metric = row['index_metric'].values[0]
            index_economy_metric = basic_data.loc[(basic_data['date'] == index_date) & 
                                        (basic_data['territory'] == index_territory)]['economy_metric'].values[0]
            df[(company, subcategory)] = (index_metric * (df['economy_metric'] / index_economy_metric)) * df['exchange_rate']
        else:
            print(f"No matching row for company {company} and subcategory {subcategory}")

        i += 1
    
    print("Metrics calculated.")
    df['year'] = df['date'].dt.year
    df['period'] = df['date'].dt.quarter

    return df

def main(master_table=master_table):
    basic_data = basic_data_export()[['date','time','region','territory','economy_metric','currency','exchange_rate']]
    print("")
    df = metric_calculator(basic_data=basic_data, master_table=master_table)
    return df
