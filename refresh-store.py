#!/usr/bin/python

import urllib2
import json
import sys
from optparse import OptionParser

usage = "usage: %prog [options] host"
parser = OptionParser(usage = usage)
parser.add_option("-t", "--token", dest="token", default="", type='string',
  help="token")
(options, args) = parser.parse_args()

if len(args) != 1:
  parser.print_usage()
  sys.exit(1)

host = args[0]
data = { "token": options.token }
dataStr = json.dumps(data)
print urllib2.urlopen('http://%s/private/refreshStore' % host, dataStr).read()