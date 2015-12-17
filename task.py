from celery import Celery
import json
from collections import Counter
import urllib2
import subprocess
from celery import group
import time

app = Flask(__name__)

@app.route('/', methods=['GET'])
def cow_say():
    data = count()
    return data

if __name__ == '__main__':    
    app.run(host='0.0.0.0',debug=True)


app  = Celery('tasks', backend= 'amqp', broker = 'amqp://')

@app.task
def count1():
  mesh = []
  req = urllib2.Request("http://smog.uppmax.uu.se:8080/swift/v1/tweets")
  response = urllib2.urlopen(req)
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

  print "Celery is working..."
  counter = 0
  while (meshTask.ready() == False):
      print "... %i s" %(counter)
      counter += 5
      time.sleep(5)

  results = meshTask.get()
  names = []
  print results


  @app.task
  def count():
    c0 = read('tweets_0.txt')
    c1 = read('tweets_1.txt')
    c2 = read('tweets_2.txt')
    c3 = read('tweets_3.txt')
    c4 = read('tweets_4.txt')
    c5 = read('tweets_5.txt')
    c6 = read('tweets_6.txt')
    c7 = read('tweets_7.txt')
    c8 = read('tweets_8.txt')
    c9 = read('tweets_9.txt')
    c10 = read('tweets_10.txt')
    c11 = read('tweets_11.txt')
    c12 = read('tweets_12.txt')
    c13 = read('tweets_13.txt')
    c14 = read('tweets_14.txt')
    c15 = read('tweets_15.txt')
    c16 = read('tweets_16.txt')
    c17 = read('tweets_17.txt')
    c18 = read('tweets_18.txt')
    c19 = read('tweets_19.txt')


    hon = c0[0]+c1[0]+c2[0]+c3[0]+c4[0]+c5[0]+c6[0]+c7[0]+c8[0]+c9[0]+c10[0]+c11[0]+c12[0]+c13[0]+c14[0]+c15[0]+c16[0]+c17[0]+c18[0]+c19[0]
    han = c0[1]+c1[1]+c2[1]+c3[1]+c4[1]+c5[1]+c6[1]+c7[1]+c8[1]+c9[1]+c10[1]+c11[1]+c12[1]+c13[1]+c14[1]+c15[1]+c16[1]+c17[1]+c18[1]+c19[1]
    hen = c0[2]+c1[2]+c2[2]+c3[2]+c4[2]+c5[2]+c6[2]+c7[2]+c8[2]+c9[2]+c10[2]+c11[2]+c12[2]+c13[2]+c14[2]+c15[2]+c16[2]+c17[2]+c18[2]+c19[2]
    den = c0[3]+c1[3]+c2[3]+c3[3]+c4[3]+c5[3]+c6[3]+c7[3]+c8[3]+c9[3]+c10[3]+c11[3]+c12[3]+c13[3]+c14[3]+c15[3]+c16[3]+c17[3]+c18[3]+c19[3]
    denna = c0[4]+c1[4]+c2[4]+c3[4]+c4[4]+c5[4]+c6[4]+c7[4]+c8[4]+c9[4]+c10[4]+c11[4]+c12[4]+c13[4]+c14[4]+c15[4]+c16[4]+c17[4]+c18[4]+c19[4]
    denne = c0[5]+c1[5]+c2[5]+c3[5]+c4[5]+c5[5]+c6[5]+c7[5]+c8[5]+c9[5]+c10[5]+c11[5]+c12[5]+c13[5]+c14[5]+c15[5]+c16[5]+c17[5]+c18[5]+c19[5]
    det = c0[6]+c1[6]+c2[6]+c3[6]+c4[6]+c5[6]+c6[6]+c7[6]+c8[6]+c9[6]+c10[6]+c11[6]+c12[6]+c13[6]+c14[6]+c15[6]+c16[6]+c17[6]+c18[6]+c19[6]
    total = c0[7]+c1[7]+c2[7]+c3[7]+c4[7]+c5[7]+c6[7]+c7[7]+c8[7]+c9[7]+c10[7]+c11[7]+c12[7]+c13[7]+c14[7]+c15[7]+c16[7]+c17[7]+c18[7]+c19[7]
    print 'hon', hon
    print 'han', han
    print 'hen', hen
    print 'den', den
    print 'denna', denna
    print 'denne', denne
    print 'det', det
    print 'total', total


  @app.task
  def read(file):
    import re
    f = open(file, "r")
    hon_c = 0
    han_c = 0
    hen_c = 0
    den_c = 0
    denna_c = 0
    denne_c = 0
    det_c = 0
    total = 0

    for line in f:
        if len(line) <= 1:
                continue
        tweet = json.loads(line)
        #print line
        if tweet['retweeted'] == False:
                text = tweet['text'].encode('UTF-8', 'replace')
                text = text.strip()
                #print text
                text_split = text.split()
                hon = 'hon' in text_split
                C = Counter(text_split)
                hon_c += C['hon']
                han = 'han' in text_split
                han_c += C['han']
                hen_c += C['hen']
                den_c += C['den']
                det_c += C['det']
                denna_c += C['denna']
                denne_c += C['denne']

  total += hon_c + han_c + hen_c + den_c + denna_c + denne_c + det_c
  print total
  f.close()
  return [hon_c, han_c, hen_c, den_c, denna_c, denne_c, det_c, total]

