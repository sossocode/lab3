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
    import collections
    words = re.findall(r'\w+', open('tweets_1.txt').read().lower())
    c = collections.Counter(words)
    count = []
    print c['id']
    print 'hon:', c['hon']
    print 'han:', c['han']
    print 'hen:', c['hen']
    print 'den:', c['den']
    print 'denna:', c['denna']
    print 'denne:', c['denne']
    print 'det:', c['det']
    total = c['hon'] + c['han']+ c['hen'] +c['den'] +c['denna'] +c['denne'] +c['det']
    print 'total', total

