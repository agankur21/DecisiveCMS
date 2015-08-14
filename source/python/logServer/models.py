from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model


class Logs(Model):
 id = columns.UUID(primary_key=True)
 product_id  = columns.Text()
 ip = columns.Text()
 time = columns.Text()
