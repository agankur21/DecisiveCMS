from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
import uuid


class Logs(Model):
    id = columns.UUID(primary_key=True, default=uuid.uuid4)
    product_id  = columns.Text()
    ip = columns.Text()
    date = columns.Text(primary_key=True,clustering_order="DESC")
    time = columns.BigInt (primary_key=True,clustering_order="DESC")
    page_id= columns.Text()
    cookie_id= columns.Text()
    user_agent=columns.Text()
    author = columns.Text()
    category=columns.Text()
    event_where=columns.Text()
    event_desc =columns.Text()
    referer_url =columns.Text()
    referer_domain= columns.Text()




class IP(Model):
    ip = columns.Text(primary_key=True)
    latitude = columns.Float();
    longitude = columns.Float();
    city = columns.Text();
    region = columns.Text();
    country = columns.Text();


class UserAgent(Model):
    ua = columns.Text(primary_key=True)
    browser = columns.Text();
    browser_version = columns.Text();
    os = columns.Text();
    os_version= columns.Text();
    device= columns.Text();
    device_type   = columns.Text();

class UnresolvedIP(Model):
    ip = columns.Text(primary_key=True)



