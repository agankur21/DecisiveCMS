#!flask/bin/python
from flask import Flask, jsonify, abort, request, make_response, url_for
import os


app = Flask(__name__, static_url_path = "")


logs = [
    {
        'id': 1,
        'product_id': u'12345',
        'time': u'123456',
        'ip': u'12.45.67.2',
        'event_type':u'click',
        'page_id':u'absd',
        'user_agent':u'',

    },
    {
        'id': 2,
        'product_id': u'12345',
        'time': u'123456',
        'ip': u'12.45.67.2'
    }
]

@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)

@app.route('/api/logs', methods=['POST'])
def create_log():
    if not request.json or not 'product_id' in request.json:
        abort(400)
    task = {
        'product_id': request.json['product_id'],
        'time': request.json.get('time', ""),
        'ip': request.json.get('ip', ""),
        'event_type': request.json.get('event_type', ""),
        'page_id': request.json.get('page_id', ""),
        'cookie_id': request.json.get('cookie_id', ""),
        'event_properties': request.json.get('event_properties', ""),
        'user_agent': request.json.get('user_agent', ""),
        'referer_info': request.json.get('referer_info', "")

    }
    logs.append(task)
    if os.path.exists("log-file.txt"):
        with open("log-file.txt", "a") as myfile:
            myfile.write(str(task))
    else:
        with open('log-file.txt', 'w') as f:
            f.write(str(task))


    return jsonify({'task': task}), 201

if __name__ == '__main__':
    app.run(debug=True)
