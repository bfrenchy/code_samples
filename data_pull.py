def data_export(t1=TableOne, t2=TableTwo, t3=TableThree):
    """
    Export the basic data from the database into a dataframe.
    It connects to the database, selects certain columns from joined tables, and manipulates the resulting dataframe.
    
    Parameters:
    - t1 (SQLAlchemy Table): TableOne.
    - t2 (SQLAlchemy Table): TableTwo.
    - t3 (SQLAlchemy Table): TableThree.
    
    Returns:
    - data_df (pandas.DataFrame): DataFrame with selected data for specified categories and date range.
    """

    session = establish_connection()

    print("Building SQL query for data...")
    stmt = select([
        t2.category,
        t2.attribute,
        t3.region,  
        t2.time, 
        t2.measure1,
        t1.rate,
        t2.measure2,
        label('computed_measure1', t2.measure2 / t1.rate),
        label('computed_measure2', t2.measure2 / t2.measure3),
        label('computed_measure3', (t2.measure2 / t1.rate) / t2.measure3),
        t2.measure3,
        t2.measure4,
        t2.measure5,
        t2.measure6,
        t1.rate_type,
        t1.output_attr,
        t2.update_time,
        label('last_db_pull', func.current_date())
    ]).select_from(
        t2.__table__
        .join(t1.__table__, (t2.time == t1.time) & (t2.attribute == t1.input_attr))
        .join(t3.__table__, t2.category == t3.category)
    ).where(
        t1.rate_type == 'Fixed',
        t2.time.in_(time_range),
        t2.category.in_(categories)
    )
    
    print("Executing SQL query for data and loading the results into a dataframe...")
    data_df = pd.read_sql(stmt, session.bind)

    print("Closing database connection...")
    session.close()

    data_df = clean_timestamps(data_df)

    data_df = data_df[['date','time','region','category','attribute','year','quarter',
                 'measure1','computed_measure2','computed_measure3','measure2','computed_measure1','measure3','measure4',
                 'measure5','measure6','rate','rate_type',
                 'output_attr','update_time','last_db_pull']]
    data_df.sort_values(['category','date'], inplace=True)
    data_df.reset_index(drop=True, inplace=True)  # reset the index to start at 0

    print("Data dataframe export complete.")
    print("")
    return data_df
