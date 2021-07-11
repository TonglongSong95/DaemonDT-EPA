import requests
from dbconnection import *
from datetime import datetime, timedelta
import json
import threading

days_to_keep = 2


# delete records that is older than days to keep variable
def truncate_older_records():
    try:
        # Connecting to the DB
        print('Starting the connection.')
        connection = dbconnection().connect()
        cur = connection.cursor()
        dt = datetime.now() - timedelta(days=days_to_keep)
        # delete template
        sql_del_template = f"DELETE from epa.air_monitor_data WHERE since_time < '{dt}';"
        print('Truncating Older Records')
        try:
            cur.execute(sql_del_template)
        except (Exception) as err:
            print(err)
        finally:
            connection.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            cur.close()
            connection.close()
            print('Connection is cleaned up.')


# function that takes json data from epa and insert them into dt database
def insert_epa_data(data):
    for record in data['records']:
        try:
            # Connecting to the DB
            print('connecting to DT')
            connection = dbconnection().connect()
            cur = connection.cursor()
            print('connection successful')
            # insert each parameter
            site_id = record['siteID']
            site_name = record['siteName']
            print(f"inserting {site_name}")
            longitude = record['geometry']['coordinates'][0]
            latitude = record['geometry']['coordinates'][1]
            site_type = record['siteType']
            since_time = record['siteHealthAdvices'][0]['since']
            until_time = record['siteHealthAdvices'][0]['until']
            raw_data = json.dumps(record['siteHealthAdvices'][0])
            sql_del_template = f"DELETE from epa.air_monitor_site WHERE site_id = '{site_id}';"
            cur.execute(sql_del_template)
            site_template = """INSERT INTO epa.air_monitor_site (site_id, site_name, longitude, latitude, site_type) VALUES (%s, %s, %s, %s, %s);"""
            cur.execute(site_template, (site_id, site_name, longitude, latitude, site_type))
            data_template = """INSERT INTO epa.air_monitor_data (site_id, since_time, until_time, raw_data) VALUES (%s, %s, %s, %s);"""
            cur.execute(data_template, (site_id, since_time, until_time, raw_data))
            connection.commit()
            print('data committed')
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if connection is not None:
                cur.close()
                connection.close()
                print('Connection is cleaned up.')

def daemon():
    try:
        truncate_older_records()
        api_key = '27db2814cda04ecf8e026c1542ad612a'
        url = 'https://gateway.api.epa.vic.gov.au/environmentMonitoring/v1/sites?environmentalSegment=air'
        # getting data from epa api
        r = requests.get(url, headers={
            # user-agent is needed otherwise epa refused to give a response
            'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'X-API-Key': api_key
        })

        data = r.json()
        insert_epa_data(data)

        # run it every 60 minutes
        threading.Timer(3600, daemon).start()

    except (Exception) as error:
        print(error)
    finally:
        print("=== wait for next round")


daemon()






