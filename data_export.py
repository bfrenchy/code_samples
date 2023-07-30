def data_export(t1=TableOne, t2=TableTwo, t3=TableThree):
    """
    Export the data from the database into a dataframe.

    This function connects to the database, executes a SQL query to retrieve the data from joined tables,
    and manipulates the resulting data in a pandas dataframe. The dataframe is then cleaned, sorted, and finally returned. 

    Parameters:
    - t1 (SQLAlchemy Table): First table.
    - t2 (SQLAlchemy Table): Second table.
    - t3 (SQLAlchemy Table): Third table.

    Returns:
    - data_df (pandas.DataFrame): DataFrame with data for specified parameters and date range.
    """
    session = establish_connection()
    
    print("Building SQL query for data...")
    # Create subquery for total subscriptions and revenue
    subq = select([
        t1.time,
        t1.territory,
        func.sum(t1.subscriptions).label('total_subs'),
        func.sum(t1.revenue).label('total_revenue_loc')
    ]).where(
        t1.business_line == 'Business Line'
    ).group_by(
        t1.time, t1.territory
    ).alias('total')

    stmt = select([
        t1.time,
        t1.territory,
        t2.region,
        t1.company,
        t1.business_line,
        func.max(t1.subscriptions).label('subscriptions'),
        subq.c.total_subs,
        label('subscriptions_market_share', (func.max(t1.subscriptions) / subq.c.total_subs)),
        func.max(t1.revenue).label('revenue_loc'),
        label('revenue_usd', (func.max(t1.revenue) / t3.exchange_rate)),
        subq.c.total_revenue_loc,
        label('total_revenue_usd', (func.max(t1.revenue) / t3.exchange_rate)),
        label('revenue_market_share', (func.max(t1.revenue) / subq.c.total_revenue_loc)),
        t1.currency,
        t3.output_currency,
        t3.exchange_rate,
        t3.exchange_rate_type,
        t1.forecast_flag,
        t1.update_time,
        label('last_db_pull', func.current_date())   
    ]).select_from(
        t1.__table__
        .join(t3.__table__, (t1.time == t3.time) & (t1.currency == t3.input_currency))
        .join(t2.__table__, t1.territory == t2.country)
        .join(subq, and_(t1.time == subq.c.time, t1.territory == subq.c.territory))  # Join with the subquery
    ).where(
        t3.exchange_rate_type == 'Fixed',
        t1.time.in_(time_range),
        t1.territory.in_(countries),
        t1.business_line == 'Business Line'
    ).group_by(
        t1.time, t1.territory, t2.region, t1.company, t1.business_line,
        t1.currency, t3.output_currency, t3.exchange_rate, t3.exchange_rate_type,
        t1.forecast_flag, t1.update_time, subq.c.total_subs, subq.c.total_revenue_loc
    )
    
    print("Executing SQL query for data and loading the results into a dataframe...")
    data_df = pd.read_sql(stmt, session.bind)

    print("Closing database connection...")
    session.close()

    print("Cleaning and sorting the dataframe...")
    data_df = clean_timestamps(data_df)
    data_df.sort_values(['territory','date'], inplace=True)
    data_df.reset_index(drop=True, inplace=True)  # reset the index to start at 0

    # Reorder the columns
    cols = data_df.columns.tolist()
    cols = cols[-3:] + cols[:-3]
    cols.insert(1, cols.pop(3))
    data_df = data_df[cols]

    print("Data dataframe export complete.")
    print("")
    return data_df
  
