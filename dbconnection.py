'''
CDMPS database connection module
Python Version 2.7.16
Requirements: psycopg2 2.8.4
Developer: Shadi Masoumi
Date: 06/03/2020
'''

import psycopg2

class dbconnection:
    def __init__(self):
        # DB credentials
        self.hostname = '45.113.232.131'
        self.username = 'obs_user'
        self.password = '6rd6hre4tc'
        self.database = 'observation2_db'

    def connect(self):
        try:
            connection = psycopg2.connect(host=self.hostname, user=self.username, password=self.password,
                                          dbname=self.database, connect_timeout=10, keepalives=1, keepalives_idle=20,
                                          keepalives_interval=5)
            return connection
        except Exception:
            raise