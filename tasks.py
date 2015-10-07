#!/usr/bin/python
# -*- coding: utf-8 -*-
from celery import Celery
import json

app  = Celery('tasks', backend= 'amqp', broker = 'amqp://')


@app.task
def read_line():
    import re
    f = open('tweets_0.txt', "r")
    for line in f:
    #line = f.readline()
        tweet = json.loads(line)
        if tweet['retweeted'] == False:
                text = tweet['text'].encode('UTF-8', 'replace')
                print text
                if text.find('hej'):
                        print 'found'
                else:
                        print 'not found'
        #match = re.search('hon', tweet)
        #print match
        #tweets_hon = tweet['text'].apply(lambda tweet: word_in_text('python', $

@app.task
def test():
    import re
    f = open('tweets_1.txt', "r")
    #line = f.readlines()
    for line in f:
        #print line
        tweet = json.loads(line)
        print tweet
        text  = tweet['text'].encode('UTF-8', 'replace')
        print text
