import cassandra
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, SimpleStatement
from cassandra.cluster import Cluster
from cassandra.policies import *

from prettytable import PrettyTable
import time
import ssl
from ssl import PROTOCOL_TLSv1_2
from requests.utils import DEFAULT_CA_BUNDLE_PATH
import os, sys, json
import logging
import azure.functions as func

#
# Supports HTTP POST
# Expects json in body of request
# 
# Example:
#  
# [
#    {"userId": 10, "userName": "john", "userCity": "bad lands"},
#    {"userId": 11, "userName": "george", "userCity": "concrete jungle"},
#    {"userId": 12, "userName": "dalsim", "userCity": "sin city"}
# ]
# 
# When running locally, this function will pause when debugging due to inability to resolve
# cassandra-driver modules -- disregard these errors and continue debugging.
#
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    session = new_session(3)

    #<createKeyspace>
    logging.info("Creating Keyspace")
    session.execute(
        'CREATE KEYSPACE IF NOT EXISTS uprofile WITH replication = {\'class\': \'NetworkTopologyStrategy\', \'datacenter\' : \'1\' }')
    #</createKeyspace>

    #<createTable>
    logging.info("Creating Table")
    session.execute(
        'CREATE TABLE IF NOT EXISTS uprofile.user (user_id int PRIMARY KEY, user_name text, user_bcity text)')
    #</createTable>
    
    #<insertRecords>
    logging.info("Insert Records")
    # session.execute("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (%s,%s,%s)", [1,'Lybkov','Seattle'])
    # session.execute("INSERT INTO  uprofile.user  (user_id, user_name , user_bcity) VALUES (%s,%s,%s)", [2,'Doniv','Dubai'])
    
    message = req.get_json()
    for row in message:
        logging.info(row)
        session.execute(
            "INSERT INTO uprofile.user (user_id, user_name, user_bcity) VALUES (%s, %s, %s)", 
            [row["userId"], row["userName"], row["userCity"]])
    #</insertRecords>

    #<queryAllItems>
    logging.info("Selecting All")
    rows = session.execute('SELECT * FROM uprofile.user')
    logging.info(rows.column_names)
    logging.info(rows.column_types)
    logging.info(rows.current_rows)
    logging.info(rows)
    #</queryAllItems>

    return func.HttpResponse(
            "all good",
            status_code=200
    )

def new_session(max_retries=3):
    attempts = 0
    delay = 3
    ssl_opts = {
                'ca_certs': DEFAULT_CA_BUNDLE_PATH,
                'ssl_version': PROTOCOL_TLSv1_2,
                }
    auth_provider = PlainTextAuthProvider(
        username=os.getenv('CASSANDRA_USERNAME'), 
        password=os.getenv('PASSWORD'))

    cluster = Cluster(
        [os.getenv('CONTACTPOINT')], 
        port = os.getenv('PORT'), 
        auth_provider=auth_provider, 
        ssl_options=ssl_opts)
   
    session = cluster.connect()
    while attempts < max_retries:
        try:
            session = cluster.connect()
            return session
        except Exception as e:
            logging.debug('Failed to create session', exc_info=e)
        time.sleep(delay)
        attempts = attempts + 1
    raise Exception('Could not establish session after {attempts}'.format(attempts=attempts))