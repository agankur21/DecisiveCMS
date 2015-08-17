import web
import os
import json
from bson import json_util
from pymongo import MongoClient
from jinja2 import Environment,FileSystemLoader
from Models import Logs
import Models
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine import connection
import time_uuid







urls = (
    '/logs', 'Logs',
    '/', 'index',
    '/donorschoose/project','Projects'

)



app = web.application(urls, globals())

MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
DBS_NAME = 'logapp'
COLLECTION_NAME = 'logs'

def _connect_to_cassandra(keyspace):
    connection.setup(['127.0.0.1'], keyspace, protocol_version=3)
    sync_table(Logs)

    return


_connect_to_cassandra('logapp')



def render_template(template_name, **context):
    extensions = context.pop('extensions', [])
    globals = context.pop('globals', {})

    jinja_env = Environment(
            loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), 'templates')),
            extensions=extensions,
            )
    jinja_env.globals.update(globals)

    return jinja_env.get_template(template_name).render(context)



class Logs:
    def POST(self):
        """
        Insert some log data into Cassandra.
        The log data is encoded as a JSON string.
        """


        value = web.data()
        value_parsed = json.loads(value)


        product_id=str(value_parsed.get('product_id',""))
        ip = str(value_parsed.get('ip',""))
        time = str(value_parsed.get('time',"123456"))
        page_id= str(value_parsed.get('page_id',""))
        cookie_id= str(value_parsed.get('cookie_id',""))
        user_agent= str(value_parsed.get('user_agent',""))

        log = Models.Logs(product_id=product_id,ip=ip,time=time,page_id=page_id,cookie_id=cookie_id,user_agent=user_agent)

        log.save()




class index:
    def GET(self):
        return render_template('index.html')


class Projects:
    """docstring for """

    def GET(self):
        # connection = MongoClient(MONGODB_HOST, MONGODB_PORT)
        # collection = connection[DBS_NAME][COLLECTION_NAME]
        # projects = collection.find(projection=FIELDS, limit=100)
        # #projects = collection.find(projection=FIELDS)
        # json_projects = []
        # for project in projects:
        #     json_projects.append(project)
        # json_projects = json.dumps(json_projects, default=json_util.default)
        # connection.close()
        return ""



if __name__ == "__main__":
    app.run()
