#!/usr/bin/env python
# aka carmendiff. cc dolapo.

import json
import urllib
import urllib2
import sys
import math


# TODO: move this to thrift

serverA = "http://prodproxy-b-2:35010/"
serverB = "http://dev-blackmad:8081/"

def getUrl(server, paramDict):
  params = urllib.urlencode(paramDict)
  return server + "?" + params

def getResponse(server, paramDict):
  response = urllib2.urlopen(getUrl(server, paramDict))
  json_response = json.loads(response.read())
  return json_response

def earthDistance(lat_1, long_1, lat_2, long_2):
  dlong = long_2 - long_1
  dlat = lat_2 - lat_1
  a = (math.sin(dlat / 2))**2 + math.cos(lat_1) * math.cos(lat_2) * (math.sin(dlong / 2))**2
  c = 2 * math.asin(min(1, math.sqrt(a)))
  dist = 3956 * c
  return dist

for line in open(sys.argv[1]):
  parts = line.strip().split("\t")
  post_params = {
    'query': parts[0]
  }

  def evallog(message):
    print '%s: %s' % (post_params['query'], message)
    print '\tserverA: %s' % getUrl(serverA, post_params)
    print '\tserverB: %s' % getUrl(serverB, post_params)

  responseA = getResponse(serverA, post_params)
  responseB = getResponse(serverB, post_params)

  if (len(responseA['interpretations']) == 0 and
      len(responseB['interpretations']) == 1):
    evallog('geocoded B, not A')

  elif (len(responseA['interpretations']) == 1 and
      len(responseB['interpretations']) == 0):
    evallog('geocoded A, not B')

  elif (len(responseA['interpretations']) and len(responseB['interpretations'])):
    interpA = responseA['interpretations'][0]
    interpB = responseB['interpretations'][0]

    if interpA['feature']['ids'] != interpB['feature']['ids']:
      evallog('ids changed')
    else:
      centerA = interpA['feature']['geometry']['center']
      centerB = interpB['feature']['geometry']['center']
      distance = earthDistance(
        centerA['lat'], 
        centerA['lng'], 
        centerB['lat'], 
        centerB['lng'])
      if distance > 0.1:
        evallog('moved by %s miles' % distance)
