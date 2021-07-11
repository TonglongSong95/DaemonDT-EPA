#!/usr/bin/env python

# make sure to install these packages before running:
# pip install pandas
# pip install sodapy
import pandas as pd
from sodapy import Socrata
from dbconnection import *
from datetime import datetime, timedelta
import threading


# change time format for request
def changetime(time):
    return time.strftime('%Y-%m-%dT%H:%M:%S') + ('.%02d' % (time.microsecond / 10000))


days_to_keep = 2
update_time = datetime.now() - timedelta(minutes=15)


# delete records that is older than days to keep variable
def truncate_older_records():
    try:
        # Connecting to the DB
        print('Starting the connection.')
        connection = dbconnection().connect()
        cur = connection.cursor()
        dt = datetime.now() - timedelta(days=days_to_keep)
        # delete template
        sql_del_template = f"DELETE from com.microclimate_sensor_readings WHERE local_time < '{dt}';"
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


# function takes pandas dataframe of com data and insert them into dt database
def insert_com_data(data):
    try:
        # connecting to dt
        print('connecting to DT')
        connection = dbconnection().connect()
        cur = connection.cursor()
        print('connection successful')
        count_insert = 0
        # insert each parameter
        for index, item in data.iterrows():
            site_id = item['site_id']
            sensor_id = item['sensor_id']
            value = item['value']
            local_time = item['local_time']
            tp = item['type']
            unit = item['units']
            data_template = """INSERT INTO com.microclimate_sensor_readings (site_id, sensor_id, value, local_time, type, units) VALUES (%s, %s, %s, %s, %s, %s);"""

            try:
                cur.execute(data_template, (site_id, sensor_id, value, local_time, tp, unit))
                count_insert += 1
            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
            finally:
                # Commiting every 25 records due to a big number of records to insert
                if (count_insert % 25 == 0):
                    connection.commit()
                    print("Inserted {} records".format(count_insert))
        print('Finilizing the process.')
        connection.commit()

        print('Finished, {} records inserted.'.format(count_insert))


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
        # Unauthenticated client only works with public data sets. Note 'None'
        # in place of application token, and no username or password:

        # Example authenticated client (needed for non-public datasets):
        # client = Socrata(data.melbourne.vic.gov.au,
        #                  MyAppToken,
        #                  userame="user@example.com",
        #                  password="AFakePassword")

        # First 2000 results, returned as JSON from API / converted to Python list of
        # dictionaries by sodapy.
        client = Socrata("data.melbourne.vic.gov.au",
                         'oCVxomRkdJEshqRqQEMJ03L2E',
                         username='digitaltwincsdila@gmail.com',
                         password='Dt123456')
        results = client.get("u4vh-84j8",
                             where=f"local_time between '{changetime(update_time)}' and '{changetime(datetime.now())}'")
        data = pd.DataFrame.from_records(results)
        insert_com_data(data)

        # run it every 15 minutes
        threading.Timer(900, daemon).start()

    except (Exception) as error:
        print(error)
    finally:
        print("=== wait for next round")


daemon()
