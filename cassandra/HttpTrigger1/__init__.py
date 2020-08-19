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


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    ssl_opts = {
                'ca_certs': DEFAULT_CA_BUNDLE_PATH,
                'ssl_version': PROTOCOL_TLSv1_2,
                }

    auth_provider = PlainTextAuthProvider(
    username=os.getenv('username'), password=os.getenv('password'))
    cluster = Cluster([os.getenv('contactPoint')], port = os.getenv('port'), auth_provider=auth_provider, ssl_options=ssl_opts)
    session = cluster.connect()

    #<createKeyspace>
    logging.info("Creating Keyspace")
    session.execute('CREATE KEYSPACE IF NOT EXISTS uprofile WITH replication = {\'class\': \'NetworkTopologyStrategy\', \'datacenter\' : \'1\' }')
    #</createKeyspace>

    #<createTable>
    logging.info("Creating Table")
    session.execute('CREATE TABLE IF NOT EXISTS uprofile.user (user_id int PRIMARY KEY, user_name text, user_bcity text)')
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
            [row["userId"], row["userName"], row["user"]])
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
