from sqlalchemy import create_engine
from pandas import DataFrame


def main():
    # ------------------------------------------------------
    # Creating connection to db
    # ------------------------------------------------------
    # postgres development db engine (registry to case_study db)
    engine = create_engine('postgresql://localhost/case_study')
    # for production database make sure you use something like:
    # engine = create_engine('postgresql://username:password@dns/database_name'))
    # also use the following if interested in using psycopg2 database api (for Oracle db's lookup cxOracle)
    # engine = create_engine('postgresql+psycopg2://username:password@dns/database_name'))

    # connection obj to postgres db engine
    connection = engine.connect()

    with open('../Results/table_names.txt', 'w') as f_table_names:
        for table_name in engine.table_names():
            f_table_names.write(table_name+'\n')
    f_table_names.close()

    # ------------------------------------------------------
    # Available tables in the db
    # ------------------------------------------------------
    skip_list = [
        # 'biannual_data_1', 'biannual_data_2',
        # 'monthly_data_1', 'monthly_data_2', 'monthly_data_3'
        # ,'monthly_data_4', 'monthly_data_5', 'monthly_data_6', 'monthly_data_7', 'monthly_data_8'
        # ,'monthly_data_9', 'monthly_data_10', 'monthly_data_11', 'monthly_data_12'
        'quarterly_data_1', 'quarterly_data_2', 'quarterly_data_3', 'quarterly_data_4'
        ,'airport', 'carrier_history', 'system_fields'
        ,'yearly_data'
    ]

    for table_name in engine.table_names():
        if table_name in skip_list:
            continue
        else:
            print(35 * '-')
            print('Current table -->', table_name)
            print(35 * '-')

            # --------------------------------------------------------------
            # Analysing data set using pandas data-frame (df) data structure
            # --------------------------------------------------------------
            #
            stmt = "SELECT * FROM {} where unique_carrier in ('EV', 'WN', 'DL') and origin in" \
                   " ('PHX','LAS','IAH','SFO','LAX','DEN','DFW','ATL','ORD','EWR','MDW','LGA');".format(table_name)
            result_proxy = connection.execute(stmt)
            results = result_proxy.fetchall()

            # select column names of current table
            stmt = "SELECT column_name FROM information_schema.columns WHERE table_name = '{}';".format(table_name)
            result_proxy = connection.execute(stmt)
            col_names = result_proxy.fetchall()
            col_names = [name[0] for name in col_names]  # remove str from tuple

            df = DataFrame(results, columns=col_names)

            print(100 * '#')

            # some df attributes
            print('\nDimensions of df\n', 50 * '-')
            print(df.shape)
            print('\nData-types of df\n', 50 * '-')
            print(df.dtypes)

            # some df methods
            print('\nTop 5 rows:\n', 50 * '-')
            print(df.head())  # see first 5 rows

            print('\ndf description (categorical columns only):\n', 50 * '-')
            cat_analytics = df.describe(include=['object'])  # 'int64', 'float64']))
            cat_analytics.to_csv('../Results/lim_cat_analytics_{}.csv'.format(table_name), encoding='utf-8')  #, index=False)
            print(cat_analytics)

            print('\ndf description (numerical columns only)\n', 50 * '-')
            num_analytics = df.describe()
            num_analytics.to_csv('../Results/lim_num_analytics_{}.csv'.format(table_name), encoding='utf-8')  #, index=False)
            print(num_analytics)

            print(100 * '#')

    connection.close()
    
if __name__ == '__main__':
    main()
