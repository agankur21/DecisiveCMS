import datetime
import json
import os
import time_uuid
import uuid
from cassandra.cluster import Cluster
from flask import Flask, request

def _connect_to_cassandra(keyspace):
    """
    Connect to the Cassandra cluster and return the session.
    """
    """
    if 'BACKEND_STORAGE_IP' in os.environ:
        host = os.environ['BACKEND_STORAGE_IP']
        cluster = Cluster([host])
    else:
        #host = '127.0.0.1:9042'
        cluster = Cluster(contact_points=['127.0.0.1',],port=9042)
    """
    #cluster = Cluster([host])
    cluster = Cluster(contact_points=['127.0.0.1',],port=9042)
    session = cluster.connect(keyspace)

    return session

"""
Create the Flask application, connect to Cassandra, and then
set up all the routes.
"""
app = Flask(__name__)
session = _connect_to_cassandra('logapp')

@app.route('/api/s', methods=['GET'])
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
               'ip': float(value_parsed['ip']) }

    session.execute(query, values)
    return ""

if __name__ == '__main__':
    app.run()
