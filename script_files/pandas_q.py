def pandas_q1(dataframe, print_flag=False):
    data = dataframe['VendorID'].value_counts().reset_index(name='count')
    if print_flag:
        print(data)
    return data

def pandas_q2(dataframe, print_flag=False):
    data = dataframe[['passenger_count', 'total_amount']].groupby('passenger_count').mean().reset_index()
    if print_flag:
        print(data)

def pandas_q3(dataframe, print_flag=False):
    data = dataframe[['passenger_count', 'tpep_pickup_datetime']]
    data['year'] = data.loc[:, 'tpep_pickup_datetime'].dt.year
    data = data.groupby(['passenger_count', 'year']).size().reset_index(name='count')
    if print_flag:
        print(data)

def pandas_q4(dataframe, print_flag=False):
    data = dataframe[['passenger_count', 'tpep_pickup_datetime', 'trip_distance']]
    data['trip_distance'] = data['trip_distance'].round().astype(int)
    data['year'] = data.loc[:, 'tpep_pickup_datetime'].dt.year
    data = data.groupby(['passenger_count', 'year', 'trip_distance']).size().reset_index(
        name='count').sort_values(['year', 'count'], ascending=[True, False]
    )
    if print_flag:
        print(data)

