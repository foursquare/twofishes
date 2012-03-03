#!/usr/bin/python

import json
import urllib
from geotools import *
import sys

failures = open('eval-failures.html', 'w')
successes = open('eval-success.html', 'w')
likely = open('eval-likely.html', 'w')
ambiguous = open('eval-ambiguous.html', 'w')

if len(sys.argv) > 1:
  f = open(sys.argv[1])
else:
  f = sys.stdin

count = 0
for line in f:
  count += 1
  if count % 100 == 0:
    print "processed: %d" % count
    failures.flush()
    successes.flush()

  line = line.strip()
  params = urllib.urlencode({'query': line})
  url = 'http://dev-blackmad:8889/?%s' % params
  u = urllib.urlopen(url)
  data = u.read()
  try:
    resp = json.loads(data)['resp']
    if len(resp) == 0:
      failures.write('<a href="%s">%s</a><br/>\n' % (url, line))
    else:
      successes.write('<a href="%s">%s</a>: %s<br/>\n' % (url, line, len(resp)))
  except:
    print url
    print data
  
