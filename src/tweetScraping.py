import argparse
import sqlalchemy
import pandas as pd

def main(params):
    user       = params.user 
    password   = params.password
    host       = params.host
    port       = params.port
    db         = params.db
    table_name = params.table_name
    
    csv_name = '../data/espacegg.csv'

    engine = sqlalchemy.create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    df = pd.read_csv(csv_name)

    df = df[['Date', 'TweetURL', 'User', 'Source', 'Location', 'Tweet',
        'Likes_Count', 'Retweet_Count', 'Quote_Count', 'Reply_Count']]
    
    df.Date = pd.to_datetime(df.Date)


    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(description="Ingesting csv data in Postgres")

    # user, password, host, port, databasename, table name, url of the csv
    parser.add_argument('user', help='user name for postgres')
    parser.add_argument('password', help='password for postgres')
    parser.add_argument('host', help='host name for postgres')
    parser.add_argument('port', help='port number for postgres')
    parser.add_argument('db', help='database name for postgres')
    parser.add_argument('table_name', help='table name for postgres where we will write the results to')
    parser.add_argument('url', help='url of the csv file')
    
    args = parser.parse_args()
    
    main(args)





