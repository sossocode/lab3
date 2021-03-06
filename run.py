from celery import Celery
import json
from collections import Counter
import urllib2
import subprocess
from celery import group
import time

app = Flask(__name__)

@app.route('/', methods=['GET'])
def runs():
  mesh = []
  tweets = urllib2.Request("http://smog.uppmax.uu.se:8080/swift/v1/tweets")
  response = urllib2.urlopen(tweets)
  meshObject = response.read().split()
  for t in meshObject:
     mesh.append(t)

  A = mesh[:4]
  print A
  B = mesh[4:8]
  C = mesh[8:12]
  D = mesh[12:14]
  E = mesh[14:16]
  F = mesh[16:]

  job = group(read.s(A),
        read.s(B),
        read.s(C),
        read.s(D),
        read.s(E),
        read.s(F))

  meshTask = job.apply_async()

  results = meshTask.get()
  print results


if __name__ == '__main__':    
    app.run(host='0.0.0.0',debug=True)
