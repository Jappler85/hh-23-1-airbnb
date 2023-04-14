
from dotenv import dotenv_values
import pandas as pd
import sqlalchemy


def get_sql_config():
    '''
        Function loads credentials from .env file and
        returns a dictionary containing the data needed for sqlalchemy.create_engine()
    '''
    needed_keys = ['host', 'port', 'database', 'user', 'password']
    dotenv_dict = dotenv_values(".env")
    sql_config = {key: dotenv_dict[key]
                  for key in needed_keys if key in dotenv_dict}
    return sql_config


def get_data(sql_query):
    '''
        Runs a query and gives output as list with fetchall method.
    '''
    engine = get_engine()
    with engine.begin() as conn:
        results = conn.execute(sql_query)
        return results.fetchall()


def get_dataframe(sql_query):
    ''' 
        Connect to the PostgreSQL database server, 
        run query and return data as a pandas dataframe
    '''
    engine = get_engine()
    return pd.read_sql_query(sql=sql_query, con=engine)


def get_engine():
    '''
        Returns an engine object to connect to a postgresql database using sqlalchemy
    '''
    sql_config = get_sql_config()
    return sqlalchemy.create_engine('postgresql://user:pass@host/database', connect_args=sql_config)


def increase_bbox(dataframe):
    """Increases max and decreases min of four longitude/latitude coordinates 
    for a box by 0.01, or a little more than 1km, each."""
    north = dataframe.describe()["latitude"].loc["max"]
    south = dataframe.describe()["latitude"].loc["min"]
    west = dataframe.describe()["longitude"].loc["min"]
    east = dataframe.describe()["longitude"].loc["max"]
    return {"north_shifted": north+0.01, "south_shifted": south-0.01, "west_shifted": west-0.01, "east_shifted": east+0.01}
