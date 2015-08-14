import datetime
import json
import os
import time_uuid
import uuid
from cassandra.cluster import Cluster
from flask import Flask, request
from cassandra.cqlengine import connection
from cassandra.cqlengine.management import sync_table
from models import Logs

def _connect_to_cassandra(keyspace):

    #cluster = Cluster(contact_points=['127.0.0.1',],port=9042,protocol_version=3)
    #session = cluster.connect(keyspace)
    connection.setup(['127.0.0.1'], "keyspace", protocol_version=3)
    sync_table(Logs)

    return session

"""
Create the Flask application, connect to Cassandra, and then
set up all the routes.
"""
app = Flask(__name__)
#session = _connect_to_cassandra('logapp')

@app.route('/api/logs', methods=['GET'])
def get_logs():
    """
    Fetch the log data from the Cassandra cluster.
    The user can filter by log, and the number
    of days in the past.
    """

    log = str(request.args['log'])
    days = int(request.args['days'])

    day = datetime.datetime.utcnow() - datetime.timedelta(days=days)
    ts = time_uuid.TimeUUID.with_utc(day, randomize=False, lowest_val=True)

    query = """SELECT * FROM log_data
               WHERE log_id=%(log)s
               and time>%(time)s
               ORDER BY time DESC"""

    values = { 'time': ts,
               'log': log }

    rows = session.execute(query, values)
    reply = { 'rows' : [] }
    for r in rows:
        ts = time_uuid.TimeUUID(str(r.time))
        dt = str(ts.get_datetime())
        reply['rows'].append({ 'log' : str(r.log_id),
                               'ip': str(r.ip),
                               'time': str(dt) })
    return json.dumps(reply)

@app.route('/api/logs', methods=['POST'])
def put_logs():
    """
    Insert some log data into Cassandra.
    The log data is encoded as a JSON string.
    """

    value = request.form['value']
    value_parsed = json.loads(value)

    query = """INSERT INTO log_data
               (log_id, time, ip)
               VALUES (%(log_id)s, %(time)s, %(ip)s)"""

    values = { 'log_id':str(value_parsed['log']),
               'time': time_uuid.TimeUUID(value_parsed['time']),
               'ip': str(value_parsed['ip']) }

    Logs.create(product_id=str(value_parsed['log']),ip=str(value_parsed['ip']),time=str(value_parsed['ip']))
    #session.execute(query, values)
    return ""

if __name__ == '__main__':
    app.run()