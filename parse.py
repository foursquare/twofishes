#!/usr/bin/python

import os
import sys
from optparse import OptionParser
import datetime

usage = "usage: %prog [options] output_directory"
parser = OptionParser(usage = usage)
parser.add_option("-w", "--world", dest="world", action="store_true",  default=True,
  help="parse world")
parser.add_option("-c", "--country", dest="country",  default='',
  help="parse country")

(options, args) = parser.parse_args()

basepath = '.'
if len(args) != 0:
  basepath = args[0]
basepath = os.path.join(basepath, str(datetime.datetime.now()).replace(' ', '-'))
print "outputting index to %s" % basepath
os.mkdir(basepath)

if options.country:
  cmd_opts = '--parse_country %s' % options.country
else:
  cmd_opts = '--parse_world true'
  
cmd = './sbt "indexer/run-main com.foursquare.twofishes.importers.geonames.GeonamesParser %s --hfile_basepath %s"' % (cmd_opts, basepath)
print(cmd)
os.system(cmd)

