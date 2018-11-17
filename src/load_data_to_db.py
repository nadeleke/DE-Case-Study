import re
import ast
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
from table_classes import Declarative_Base, Airport, Carrier_History, System_Fields

print(Declarative_Base)

# ------------------------------------------------------
# Main
# ------------------------------------------------------
def main():
    # postgres development db engine (registry to case_study db)
    engine = create_engine('postgresql://localhost/case_study')
    # for production database make sure you use something like:
    # engine = create_engine('postgresql://username:password@dns/database_name'))
    # also use the following if interested in using psycopg2 database api (for Oracle db's lookup cxOracle)
    # engine = create_engine('postgresql+psycopg2://username:password@dns/database_name'))

    # configuring a "Session" class bound to db engine
    Session = sessionmaker(bind=engine)

    # creating a Session
    session = Session()

    # ------------------------------------------------------
    # Loading tables to db
    # ------------------------------------------------------

    for i in range(1, 13):
        with open('../Monthly Data/{}.csv'.format(i), 'r') as f:
            df = pd.read_csv(f, index_col=0)
            df = df.loc[:, ~ df.columns.str.contains('Unnamed')]
            df.columns = [c.lower() for c in df.columns]  # removing capital letters
            # print(df)
            # df.to_sql("monthly_data_{}".format(i), engine)

        f.close()

    with open('../Lookup Tables/L_AIRPORT_ID.csv', 'r') as f:

        count = 0
        next(f)  # skips the first row
        for line in f:
            count += 1
            # tokenizing and separating
            tokens = line.split(',', 1)
            ID = ast.literal_eval(tokens[0])

            tokens = ast.literal_eval(tokens[1]).split(':', 1)
            location = tokens[0]

            try:
                airport_name = tokens[1]
            except Exception:
                airport_name = 'UNKNOWN'

            tokens = re.findall(r"\b[A-Z]{2}\b", location)
            if tokens:
                state = tokens[0]
                country = 'United States'
                tokens = re.split(r",\s[A-Z]{2}\b", location)
                city = tokens[0]
            else:
                state = None
                tokens = location.split(',')
                country = tokens[-1]
                city = tokens[0]

                # print(count, ID, airport_name, city, state, country, location)
                #
                # data = Airport(ID, airport_name, city, state, country, location)
                # session.add(data)
                # session.commit()

    f.close()

    with open('../Lookup Tables/L_CARRIER_HISTORY.csv', 'r') as f:

        count = 0
        next(f)  # skips the first row
        for line in f:
            count += 1

            # literal evaluation
            line = ast.literal_eval(line)
            code = line[0]

            # using regex to find years
            years = re.findall(r'\b\d{4}\b', line[1])
            if len(years) == 1:
                start_year = ast.literal_eval(years[0])
                end_year = None
            else:
                start_year = ast.literal_eval(years[0])
                end_year = ast.literal_eval(years[1])

            # using regex to split lines and tokenize description
            tokens = re.split(r'\s\(\d{4}', line[1])
            description = tokens[0]

            unique_code = code + str(start_year) + str(end_year) if end_year else code + str(start_year)

            # print(count, unique_code, code, description, start_year, end_year)

            # data = Carrier_History(unique_code, code, description, start_year, end_year)
            # session.add(data)
            # session.commit()

    f.close()

    with open('../Monthly Data/ReadMe.txt', 'r') as f:

        count = 0
        next(f)  # skips the first row
        for line in f:
            count += 1

            # literal evaluation
            line = ast.literal_eval(line)
            field = line[0]
            description = line[1]

            # print(count, field.lower(), description)  # , 'type=', type(start_year), 'type=', type(end_year))

            # data = System_Fields(field.lower(), description)
            # session.add(data)
            # session.commit()

    f.close()

    # ------------------------------------------------------
    # Create orm built tables and show all tables
    # ------------------------------------------------------

    # Create all tables using table classes (if not exist)
    Declarative_Base.metadata.create_all(bind=engine)

    # See table names
    print('\nTable names:')
    for table_name in engine.table_names():
        print(table_name)

    # # initializing metadata obj
    # metadata = MetaData()

    # # table instances
    # airport = Table('airport', metadata, autoload=True, autoload_with=engine)
    # carrier_hist = Table('carrier_history', metadata, autoload=True, autoload_with=engine)
    # system_fields = Table('system_fields', metadata, autoload=True, autoload_with=engine)

    # # Print table metadata
    # print('\nairport table\t', repr(airport))
    # print('\ncarrier_hist table:\t', repr(carrier_hist))
    # print('\nsystem_fields table:\t', repr(system_fields))
    # print(100 * '-')

    # Creating table from existing
    stmt = """
    CREATE TABLE yearly_data AS
    SELECT * FROM {0} t1
     JOIN {1} t2 using fl_date;
    """.format(engine.table_names())

    connection = engine.connect()
    result_proxy = connection.execute(stmt)
    results = result_proxy.fetchall()
    connection.close()


# ------------------------------------------------------
# Run process entry point
# ------------------------------------------------------
if __name__ == '__main__':
    main()
