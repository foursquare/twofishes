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
import traceback
from optparse import OptionParser

# TODO: move this to thrift

parser = OptionParser(usage="%prog [input_file]")
parser.add_option("-o", "--old", dest="serverOld")
parser.add_option("-n", "--new", dest="serverNew")
(options, args) = parser.parse_args()

if not options.serverOld:
  print 'missing old server'
  parser.print_usage()
  sys.exit(1)

if not options.serverNew:
  print 'missing new server'
  parser.print_usage()
  sys.exit(1)

if len(args) != 1:
  print 'weird number of remaining args'
  parser.print_usage()
  sys.exit(1)

inputFile = args[0]

outputFile = open('eval-%s.html' % datetime.datetime.now(), 'w')

def getUrl(server, param):
  if not server.startswith('http'):
    server = 'http://%s' % server
  return server.rstrip('/') + '/' + param.lstrip('/')

def getResponse(server, param):
  try:
    response = urllib2.urlopen(getUrl(server, param))
    json_response = json.loads(response.read())
    return json_response
  except Exception as e:
    print e
    print getUrl(server, param)
    return None

# Haversine formula, see http://www.movable-type.co.uk/scripts/gis-faq-5.1.html
def earthDistance(lat_1, long_1, lat_2, long_2):
  # Convert from decimal degrees to radians.
  lat_1 = lat_1 * math.pi / 180
  lat_2 = lat_2 * math.pi / 180
  long_1 = long_1 * math.pi / 180
  long_2 = long_2 * math.pi / 180

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

      if count % 100 == 0:
        print 'processed %d queries' % count
      #print 'processed %d queries' % count
      #print 'procesing: %s' % line
      count += 1

      param = line.strip()
      import urlparse

      if '?' not in param:
        param = '?' +  urllib.urlencode([('query', param)])
      param_str = param[param.find('?')+1:]
      params = urlparse.parse_qs(param_str)

      responseOld = getResponse(options.serverOld, param)
      responseNew = getResponse(options.serverNew, param)

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
        responseKey = '%s:%s' % (getId(responseOld), getId(responseNew))
        if responseKey not in evalLogDict:
          evalLogDict[responseKey] = []

        query = ''
        if 'query' in params:
          query = params['query'][0]
        elif 'll' in params:
          query = params['ll'][0]

        message = ('%s: %s<br>' % (query, message) +
                   ' -- <a href="%s">OLD</a>' % (options.serverOld + '/static/geocoder.html#' + param_str) +
                   ' - <a href="%s">NEW</a><p>' % (options.serverNew + '/static/geocoder.html#' + param_str))
        evalLogDict[responseKey].append(message)

      if (responseOld == None and responseNew == None):
        pass
      elif (responseOld == None and responseNew != None):
        evallog('error from OLD, something from NEW')
      elif (responseNew == None and responseOld != None):
        evallog('error from NEW, something from OLD')
      elif (len(responseOld['interpretations']) == 0 and
          len(responseNew['interpretations']) > 0):
        evallog('geocoded NEW, not OLD')

      elif (len(responseOld['interpretations']) > 0 and
          len(responseNew['interpretations']) == 0):
        evallog('geocoded OLD, not NEW')

      elif (len(responseOld['interpretations']) and len(responseNew['interpretations'])):
        interpA = responseOld['interpretations'][0]
        interpB = responseNew['interpretations'][0]
        
        oldIds = [str(interp['feature']['ids'][0]) for interp in responseOld['interpretations']]
        newIds = [str(interp['feature']['ids'][0]) for interp in responseNew['interpretations']] 

        if interpA['feature']['ids'] != interpB['feature']['ids'] and \
            interpA['feature']['woeType'] != 11 and \
            interpB['feature']['woeType'] != 11 and \
            interpA['feature']['ids'] != filter(lambda x: x['source'] != 'woeid', interpB['feature']['ids']):
       	  if set(oldIds) == set(newIds):
            evallog('interp order changed %s -> %s' % (oldIds, newIds))
          else:
            evallog('ids changed %s -> %s' % (interpA['feature']['ids'], interpB['feature']['ids']))
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
            evallog('bounds in OLD, but not NEW')
          elif 'bounds' not in geomA and 'bounds' in geomB:
            evallog('bounds in NEW, but not OLD')
          elif 'bounds' in geomA and 'bounds' in geomB and geomA['bounds'] != geomB['bounds']:
            evallog('bounds differ')
          elif (len(responseOld['interpretations']) != len(responseNew['interpretations'])):
            evallog('# of interpretations differ')
          elif interpA['feature']['displayName'] != interpB['feature']['displayName']:
            evallog('displayName changed')

      self.queue.task_done()

if __name__ == '__main__':
  print "going"
  for i in range(50):
    t = GeocodeFetch(queue)
    t.setDaemon(True)
    t.start()

  for line in open(inputFile):
    queue.put(line.strip())

  queue.join()

  for k in sorted(evalLogDict, key=lambda x: -1*len(evalLogDict[x])):
    outputFile.write("%d changes\n<br/>" % len(evalLogDict[k]))
    outputFile.write(evalLogDict[k][0])

