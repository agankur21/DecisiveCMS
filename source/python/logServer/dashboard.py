import web
import os
import json
from bson import json_util
from pymongo import MongoClient
from jinja2 import Environment, FileSystemLoader
from Models import Logs , IP ,UserAgent ,UnresolvedIP
import Models
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine import connection
import time_uuid
import time
import Models;
import utils
from cassandra.cqlengine.query import DoesNotExist


urls = (
    '/logs', 'Logs',
    '/', 'index',
    '/donorschoose/project', 'Projects'

)

app = web.application(urls, globals())

MONGODB_HOST = 'localhost'
MONGODB_PORT = 27017
DBS_NAME = 'logapp'
COLLECTION_NAME = 'logs'


def _connect_to_cassandra(keyspace):
    connection.setup(['127.0.0.1'], keyspace, protocol_version=3)
    sync_table(Logs)
    sync_table(IP)
    sync_table(UserAgent)
    sync_table(UnresolvedIP)

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

        product_id = str(value_parsed.get('product_id', ""))
        ip = str(value_parsed.get('ip', ""))
        time = str(value_parsed.get('time', "123456"))
        page_id = str(value_parsed.get('page_id', ""))
        cookie_id = str(value_parsed.get('cookie_id', ""))
        user_agent = str(value_parsed.get('user_agent', ""))
        author = str(value_parsed.get('author'))
        category = str(value_parsed.get('category'))

        event_where = str(value_parsed['event_properties']['where'])
        event_desc = str(value_parsed['event_properties']['desc'])

        referer_url = str(value_parsed['referer_info']['url'])
        referer_domain = str(value_parsed['referer_info']['domain'])


        # saving log into database

        log = Models.Logs(product_id=product_id, ip=ip, time=time, page_id=page_id, cookie_id=cookie_id,
                          user_agent=user_agent, author=author, category=category, event_where=event_where,
                          event_desc=event_desc, referer_domain=referer_domain, referer_url=referer_url)
        log.save()


        # saving IP info into database
        ip_filter = Models.IP.filter(ip=ip)
        ip_obj = ip_filter.first()
        if (ip_obj is None):
            location_info = utils.getLocationInfo(ip)
            if location_info is not None:
                (region, city, country, latitude, longitude) = location_info
                ip_obj = Models.IP(ip=ip,region=region, city=city, country=country, latitude=latitude, longitude=longitude)
                ip_obj.save()
            else:
                ip_obj = Models.UnresolvedIP(ip=ip)
                ip_obj.save()



        #saving useragent into database
        ua_filter = Models.UserAgent.filter(ua=user_agent)
        ua_obj = ua_filter.first()
        if (ua_obj is None):
            (browser, browser_version, os, os_version, device, device_type) = utils.getDeviceInfo(user_agent)
            ua_obj = Models.UserAgent(ua=user_agent,browser=browser, browser_version=browser_version, os=os, os_version=os_version,
                                      device=device, device_type=device_type)
            ua_obj.save()


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
        # json_projects.append(project)
        # json_projects = json.dumps(json_projects, default=json_util.default)
        # connection.close()
        return ""


if __name__ == "__main__":
    app.run()
