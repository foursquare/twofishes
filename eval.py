#!/usr/bin/env python
# aka carmendiff. cc dolapo.

import json
import urllib
import urllib2
import sys
import math
import datetime
import Queue
import threading

# TODO: move this to thrift

serverA = "http://prodproxy-b-2:35010"
serverB = "http://dev-blackmad:8081"

outputFile = open('eval-%s.html' % datetime.datetime.now(), 'w')

def getUrl(server, param):
  return server + param

def getResponse(server, param):
  try:
    response = urllib2.urlopen(getUrl(server, param))
    json_response = json.loads(response.read())
    return json_response
  except:
    return None

def earthDistance(lat_1, long_1, lat_2, long_2):
  dlong = long_2 - long_1
  dlat = lat_2 - lat_1
  a = (math.sin(dlat / 2))**2 + math.cos(lat_1) * math.cos(lat_2) * (math.sin(dlong / 2))**2
  c = 2 * math.asin(min(1, math.sqrt(a)))
  dist = 3956 * c
  return dist

evalLogDict = {}
queue = Queue.Queue()

count = 0

class GeocodeFetch(threading.Thread):
  def __init__(self, queue):
    threading.Thread.__init__(self)
    self.queue = queue

  def run(self):
    global count
    while True:
      line = self.queue.get()

      if count % 1000 == 0:
        print 'processed %d queries' % count
      print 'processed %d queries' % count
      print 'procesing: %s' % line
      count += 1

      param = line.strip()
      import urlparse
      params = urlparse.parse_qs(param[param.find('?'):])
     
      responseA = getResponse(serverA, param)
      responseB = getResponse(serverB, param)

      def getId(response):
        if (response and 
            'interpretations' in response and
            len(response['interpretations']) and
            'feature' in response['interpretations'][0] and
            'ids' in response['interpretations'][0]['feature']):
          return response['interpretations'][0]['feature']['ids'] 
        else:
          return ''

      def evallog(message):
        responseKey = '%s:%s' % (getId(responseA), getId(responseB))
        if responseKey not in evalLogDict:
          evalLogDict[responseKey] = []
        message = ('%s: %s<br>' % (params['query'], message) +
                   ' -- <a href="%s">serverA</a>' % (serverA + '/static/geocoder.html#' + params['query'][0]) +
                   ' - <a href="%s">serverB</a><p>' % (serverB + '/static/geocoder.html#' + params['query'][0]))
        evalLogDict[responseKey].append(message)

      if (responseA == None and responseB == None):
        pass
      elif (responseA == None and responseB != None):
        evallog('error from A, something from B')
      elif (responseB == None and responseA != None):
        evallog('error from B, something from A')
      elif (len(responseA['interpretations']) == 0 and
          len(responseB['interpretations']) == 1):
        evallog('geocoded B, not A')

      elif (len(responseA['interpretations']) == 1 and
          len(responseB['interpretations']) == 0):
        evallog('geocoded A, not B')

      elif (len(responseA['interpretations']) and len(responseB['interpretations'])):
        interpA = responseA['interpretations'][0]
        interpB = responseB['interpretations'][0]

        if interpA['feature']['ids'] != interpB['feature']['ids'] and \
            interpA['feature']['woeType'] != 11 and \
            interpB['feature']['woeType'] != 11:
          evallog('ids changed')
        else:
          geomA = interpA['feature']['geometry']
          geomB = interpB['feature']['geometry']
          centerA = geomA['center']
          centerB = geomB['center']
          distance = earthDistance(
            centerA['lat'], 
            centerA['lng'], 
            centerB['lat'], 
            centerB['lng'])
          if distance > 0.1:
            evallog('moved by %s miles' % distance)
          if 'bounds' in geomA and 'bounds' not in geomB:
            evallog('bounds in A, but not B')
          elif 'bounds' not in geomA and 'bounds' in geomB:
            evallog('bounds in B, but not A')
          elif 'bounds' in geomA and 'bounds' in geomB and geomA['bounds'] != geomB['bounds']:
            evallog('bounds differ')

      self.queue.task_done()

if __name__ == '__main__':
  print "going"
  for i in range(2):
    t = GeocodeFetch(queue)
    t.setDaemon(True)
    t.start()

  for line in open(sys.argv[1]):
    queue.put(line.strip())

  queue.join()

  for k in sorted(evalLogDict, key=lambda x: -1*len(evalLogDict[x])):
    outputFile.write("%d changes\n<br/>" % len(evalLogDict[k]))
    outputFile.write(evalLogDict[k][0])

