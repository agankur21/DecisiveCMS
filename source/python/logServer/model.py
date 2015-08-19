__author__ = 'rohit'

from mongoengine import *
from Models import Logs

connect('logapp', host='localhost', port=27017)

class User(Document):
    email = StringField(required=True)
    first_name = StringField(max_length=50)
    last_name = StringField(max_length=50)
    password = StringField(max_length=200)


class Article:

    webPageViewCount =0;
    mobilePageViewCount=0;
    webPageViewBuffer=TTLBuffer (5,5,5)
    mobilePageViewBuffer=TTLBuffer (5,5,5)
    windowSize=5;                       #In minutes
    t0=0;

    def __init__(self,windowSize,t0):
        self.t0 = t0;
        self.windowSize= windowSize;
        self.webPageViewBuffer=TTLBuffer (t0,self.windowSize,self.windowSize)
        self.mobilePageViewBuffer=TTLBuffer (t0,self.windowSize,self.windowSize)

    def insertWPV(self,time):
        time = int (time /(1000*60))
        self.webPageViewBuffer.insertAt(time)

    def insertMPV(self,time):
        time = int (time /(1000*60))
        self.mobilePageViewBuffer.insertAt(time)



class TTLBuffer:

    numOfBuckets=1;
    buckets =[]
    sum =0;
    ttl =5;
    t0 = 0;

    def __init__(self , t0, ttl ):
            self.t0= t0
            self.ttl = ttl;
            self.numOfBuckets = ttl
            self.buckets = [0 for x in range(self.numOfBuckets)]

    def insertAt(self,time):
        if((self.t0 - time )>= self.ttl):
            return 0;


        if(time > self.t0):
            count =0;
            for i in range(self.t0 - self.ttl,self.t0):
                if((time - i) > self.ttl):
                   self.sum = self.sum - self.buckets[self.t0 -i-1]
                   self.buckets.pop(self.t0 -i-1)
                   count = count +1

            for i in range(count):
                self.buckets.insert(0,0)



            self.t0 = time;
            self.buckets[0]=1
            self.sum = self.sum +1;

        else:
            self.buckets[self.t0 -time] = self.buckets[self.t0 -time] +1;
            self.sum = self.sum +1;















