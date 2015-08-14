import datetime
import json
import logging
import random
import requests
import sys
import time_uuid
import uuid

REST_SERVER = 'http://127.0.0.1:5000'
NUM_FACTORIES=2
NUM_logS=3
NUM_ipS=6

def _generate_random(log_id, num_ips, distribution="uniform", days=0):
    """
    Generate some random log data, encode that data as a JSON string, and
    transmit the request to the REST server.
    """

    for k in range(num_ips):
        if distribution == "normal":
            log_value = "10.2.0.1"
        elif distribution == "extreme":
            log_value = "10.2.0.1"

        # Create a timestamp sometime in the past.
        day = datetime.datetime.utcnow() - datetime.timedelta(days=days)
        ts = time_uuid.TimeUUID.with_utc(day, randomize=False, lowest_val=True)

        # Encode our JSON values.
        values = {'value': json.dumps( { 'log':str(log_id),
                                         'time': str(ts),
                                         'ip': str(log_value)} ) }

        # Post the data to the webserver.
        requests.post(REST_SERVER + '/api/logs', data=values)

def post_data():
    """
    Generate some fake data and post it to the REST server.
    For fun, we generate three sets of data, each set representing
    a single day. Also for fun, one of those days produces more extreme data.
    """

    for i in range(NUM_FACTORIES):
        for j in range(NUM_logS):
            log_id = "FA-%d-log-%d" % (i, j)
            _generate_random(log_id, NUM_ipS, "normal",  days=2)
            _generate_random(log_id, NUM_ipS, "extreme", days=1)
            _generate_random(log_id, NUM_ipS, "normal",  days=0)

def fetch_data(log, days):
    """
    Fetch data from the REST server.
    The user can specify a specific log and
    how many days in the past to look.
    """
    values = { 'log' : log,
               'days'   : days }
    res = requests.get(REST_SERVER + '/api/logs', params=values)
    return json.loads(res.text)

def main(argv=None):
    if argv[1] == 'post':
        post_data()
    elif argv[1] == 'fetch':
        if len(argv) == 4:
            log = argv[2]
            days = int(argv[3])
        else:
            log = 'FA-0-log-0'
            days = 1

        data = fetch_data(log, days)
        print json.dumps(data,
                         sort_keys=True,
                         indent=2,
                         separators=(',',':'))

if __name__ == '__main__':
    # So that we don't get so many random warnings.
    requests_log = logging.getLogger("requests")
    requests_log.setLevel(logging.WARNING)

    main(sys.argv)
