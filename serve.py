#!/usr/bin/python

import os
import sys
from optparse import OptionParser

usage = "usage: %prog [options] hfile_directory"
parser = OptionParser(usage = usage)
parser.add_option("-p", "--port", dest="port",  default=8080, type='int',
  help="port")
parser.add_option("-r", "--rebel", dest="rebel",  default=False, action='store_true',
  help="rebel")


(options, args) = parser.parse_args()

if len(args) != 1:
  parser.print_usage()
  sys.exit(1)

basepath = args[0]

sbt = './sbt'
if options.rebel:
  sbt = './sbt-rebel'

cmd = '%s "server/run-main com.foursquare.twofishes.GeocodeFinagleServer --port %d --hfile_basepath %s"' % (sbt, options.port, basepath)
print(cmd)
os.system(cmd)

