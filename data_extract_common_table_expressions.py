def get_data(t1=TableOne, t2=TableTwo, t3=TableThree, t4=TableFour):
    """
    Function to export data based on given parameters. 

    Parameters:
    t1: TableOne.
    t2: TableTwo.
    t3: TableThree.
    t4: TableFour.

    Returns:
    A cleaned dataframe containing combined data.
    """

    session = establish_connection()

    # Core data query
    print("Querying core data...")
    size_subquery = session.query(
        t1.time_pk,
        func.count(t1.respondent_pk.distinct()).label('sample_size'),
        t1.country
    ).group_by(t1.time_pk, t1.country)

    data_subquery = session.query(
        t4.time_pk,
        t2.time,
        t3.region,
        t1.country,
        func.count(t4.respondent_pk.distinct()).label('num_users'),
        t4.online_service,
        t4.service_type_field,
        case([(t4.service_type_field == 'Type1','type1'),
              (t4.service_type_field == 'Type2','type2'),
              (t4.service_type_field == 'Type3','type3'),
              (t4.service_type_field == 'Type4','type4'),
              (t4.service_type_field == 'Type5', 'type5'),
              (t4.service_type_field == 'Type6', 'type6'),
             ], else_='other').label('service_type')
    ).join(t2, t4.time_pk == t2.pk
    ).join(t1, t4.respondent_pk == t1.respondent_pk
    ).join(t3, t1.country == t3.country
    ).filter(t4.used_in_month == 'Yes'
    ).group_by(t4.time_pk, t2.time, t3.region, t1.country, 
               t4.online_service, t4.service_type_field)
    
    # Convert to common table expressions
    n_cte = size_subquery.cte('n_cte')
    data_cte = data_subquery.cte('data_cte')

    stmt = select([
        data_cte.c.time,
        data_cte.c.region,
        data_cte.c.country.label('territory'),
        data_cte.c.online_service,
        (cast(data_cte.c.num_users, Float) / cast(n_cte.c.sample_size, Float)).label('percent_of_sample'),
        data_cte.c.num_users,
        n_cte.c.sample_size,
        data_cte.c.service_type_field,
        data_cte.c.service_type]
        ).select_from(
            data_cte.join(n_cte, and_(data_cte.c.time_pk == n_cte.c.time_pk,
                                     data_cte.c.country == n_cte.c.country))
        )

    print("Loading data into a pandas DataFrame...")
    data_df = pd.read_sql(stmt, session.bind)
    session.close()
    data_df['last_update'] = pd.to_datetime('today').strftime("%Y-%m-%d")
    data_df['forecast_flag'] = 'A'
    data_df['date'] = data_df['time'].apply(convert_quarter_to_datetime)
    # Move 'date' to second position in the columns
    cols = data_df.columns.tolist()
    cols = cols[:1] + cols[-1:] + cols[1:-1]
    data_df = data_df[cols]
    data_df.sort_values(['territory','online_service','date'], inplace=True)
    data_df.reset_index(inplace=True, drop=True)
    return data_df
