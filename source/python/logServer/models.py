from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
import uuid


class Logs(Model):
    id = columns.UUID(primary_key=True, default=uuid.uuid4)
    product_id  = columns.Text()
    ip = columns.Text()
    time = columns.Text()
    page_id= columns.Text()
    cookie_id= columns.Text()
    user_agent=columns.Text()
