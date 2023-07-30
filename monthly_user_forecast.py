"""
This module contains the functions for forecasting the number of users utilizing a certain online service.
It calculates forecasts for a number of regions from the User survey data.
It uses a logistic function with a manually specified ceiling (carrying capacity) in the Master data file.
It fits the other parameters to produce a projection out to a specified future date.

Functions:
----------
extract_user_data(): exports the user data from the database
set_global_variables(df, region): sets the global variables for the regional forecast
forecast_for_region(view, ceilings, end_date, region): produces the forecast for a given region
forecast_for_specific_region(df, region, ceilings): produces the forecast for a given region and outputs to a dataframe

Module imports:
---------------
utils.database_export_funcs.data_export: exports the user data from the database
utils.database_export_funcs.get_sample_sizes: exports the sample sizes from the database
utils.functions.create_date_range: creates a date range from the start to the end of the forecast
utils.config.user_data_ceilings: contains the long-term carrying capacities for each region
"""

import pandas as pd
import numpy as np

# Models
from scipy.optimize import curve_fit

from utils.database_export_funcs import data_export, get_sample_sizes
from utils.functions import create_date_range
from utils.config import user_data_ceilings as ceilings

regions = list(ceilings.keys())

skipped_regions = ['Region_A','Region_B','Region_C','Region_D',
                     'Region_E','Region_F','Region_G','Region_H']

def extract_user_data():
    """
    Exports user date from the database and calculates the monthly active users for each region.

    Returns
    -------
    pandas.DataFrame with monthly active users over time for each region.
    """

    print("Preparing the data for the user activity forecast...")
    df = data_export()
    sample_sizes = get_sample_sizes()
    sample_sizes.set_index(['date','region'], inplace=True)

    mau = df[['date','area','region','service_1', 'service_2',
        'service_3', 'service_4',
        'service_5', 'other_services']]

    # Get the total number of users who have used at least one service
    user_count = mau.replace(0, np.nan)  # Replace 0 with np.nan to ignore 0s in the count
    user_count = user_count.groupby(['date','area','region']).count().reset_index()
    user_count = user_count.set_index(['date','region'])

    # Add the sample sizes to user_count
    user_count['sample_size'] = sample_sizes['sample_size']
    user_count.reset_index(inplace=True)

    mau_activity = user_count.copy()

    columns = ['service_1', 'service_2','service_3', 'service_4','service_5', 'other_services']
    for col in columns:
        mau_activity[col] = mau_activity[col] / mau_activity['sample_size']
    mau_activity['forecast_flag'] = 'A'

    mau_activity.drop(columns=['sample_size'], inplace=True)

    # Add a jitter
    jitter = 1e-6
    mau_activity['service_1'] = mau_activity['service_1'] + jitter

    return mau_activity

def set_global_variables(df, region):
    """
    This function sets the global variables for the regional forecast.

    Parameters
    ----------
    df : pandas.DataFrame
        The monthly active users data from the database.
    region : str
        The region to forecast.
    
    Returns
    -------
    pandas.DataFrame
        A dataframe with the following columns:
            - date
            - region
            - service_1
            - date_num
    """
    region = region
    view = df.loc[df['region'] == region].copy()

    # Set up the forecast
    date_range, time_range = create_date_range()
    min_date = view['date'].min()
    end_date = date_range[-1]

    # Create a quarterly date range between min_date and end_date
    quarterly_dates = pd.date_range(min_date, end_date, freq='Q')

    print(f"Forecast range: {min_date.strftime('%Y-%m')} - {end_date.strftime('%Y-%m')}, quarterly intervals.")

    # Create a dataframe with the date range
    df = pd.DataFrame(quarterly_dates, columns=['date'])
    df['date'] = pd.to_datetime(df['date'])

    # Convert date to number of days since the first date in the data
    view['date'] = pd.to_datetime(view['date'])
    view = view.copy()
    view['date_num'] = (view['date'] - view['date'].min()).dt.days

    return view, end_date, min_date

def forecast_for_region(view, ceilings, end_date, region):
    """
    This function produces the forecast for a given region.

    Parameters
    ----------
    view : pandas.DataFrame
        The monthly active users data from the database.
    ceilings : dict
        The long-term carrying capacities for each region.
    end_date : datetime
        The end date of the forecast.
    region : str
        The region to forecast.

    Returns
    -------
    pandas.DataFrame
        A dataframe with the following columns:
            - date
            - region
            - service_1
            - forecast_flag
            - L
            - k
            - x0
            - t
            - formula
    """

    region = region
    ceilings = ceilings
    print(f"Building the forecast for {region}...")

    x = view['date_num']
    y = view['service_1']

    # Initial guess for the parameters ([0] is the growth rate, [1] is the midpoint)
    initial_guess = [1, 0.01]

    def logistic(t, k, x0):
        """
        L: the curve's maximum value
        k: the logistic growth rate or steepness of the curve
        x0: the x-value of the sigmoid's midpoint
        t: the time or the x-axis
        """
        L = ceilings[region]
        return L / (1 + np.exp(-k * (t - x0)))

    # Use curve_fit to find the best fit parameters
    try:
        params, cov = curve_fit(logistic, x, y, p0=initial_guess)
    except RuntimeError:
        print(f"Failed to fit the data for {region} with a logistic model.")
        params = [0, 0]

    # Create the forecast using the logistic function
    view['forecast'] = logistic(view['date_num'], *params)
    view['forecast_flag'] = 'F'
    view['L'] = ceilings[region]
    view['k'] = params[0]
    view['x0'] = params[1]

    # Create a date range for the forecast
    date_range, time_range = create_date_range()
    date_range = pd.DataFrame(date_range, columns=['date'])
    date_range['date_num'] = time_range

    # Apply the logistic function to the date range
    date_range['forecast'] = logistic(date_range['date_num'], *params)
    date_range['region'] = region
    date_range['forecast_flag'] = 'F'

    # Combine the actual data with the forecast data
    forecast = pd.concat([view, date_range])
    forecast = forecast[['date', 'region', 'service_1', 'forecast', 'forecast_flag', 'L', 'k', 'x0']]

    # Calculate the formula for the logistic function
    forecast['t'] = (forecast['date'] - forecast['date'].min()).dt.days
    forecast['formula'] = forecast['L'].map(str) + ' / (1 + exp(-' + forecast['k'].map(str) + ' * (' + forecast['t'].map(str) + ' - ' + forecast['x0'].map(str) + ')))'

    return forecast

def forecast_for_specific_region(df, region, ceilings):
    """
    This function produces the forecast for a given region and outputs to a dataframe.

    Parameters
    ----------
    df : pandas.DataFrame
        The monthly active users data from the database.
    region : str
        The region to forecast.
    ceilings : dict
        The long-term carrying capacities for each region.

    Returns
    -------
    pandas.DataFrame
        A dataframe with the following columns:
            - date
            - region
            - service_1
            - forecast
            - forecast_flag
            - L
            - k
            - x0
            - t
            - formula
    """

    view, end_date, min_date = set_global_variables(df, region)

    forecast = forecast_for_region(view, ceilings, end_date, region)
    return forecast

def main():
    df = extract_user_data()
    forecast_df = pd.DataFrame()

    for region in regions:
        if region not in skipped_regions:
            forecast = forecast_for_specific_region(df, region, ceilings)
            forecast_df = pd.concat([forecast_df, forecast])

    print("Forecasting complete.")
    return forecast_df

if __name__ == "__main__":
    main()
