__author__ = 'rohit'

from mongoengine import *

connect('logapp', host='localhost', port=27017)

class User(Document):
    email = StringField(required=True)
    first_name = StringField(max_length=50)
    last_name = StringField(max_length=50)
    password = StringField(max_length=200)
