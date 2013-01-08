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
parser.add_option("--output_prefix_index", dest="output_prefix_index",  action="store_true", default=True,
  help="output prefix hfile index to speed up autocomplete (optional)")
parser.add_option("--nooutput_prefix_index", dest="output_prefix_index",  action="store_false",
  help="don't output prefix hfile index to speed up autocomplete (optional)")
parser.add_option("-r", "--output_revgeo_index", dest="output_revgeo_index",  action="store_true", default=False,
  help="output s2 revgeo index (optional)")
parser.add_option("-n", "--dry_run", dest="dry_run",  action="store_true", default=False)

(options, args) = parser.parse_args()

basepath = '.'
if len(args) != 0:
  basepath = args[0]
basepath = os.path.join(basepath, str(datetime.datetime.now()).replace(' ', '-').replace(':', '-'))
print "outputting index to %s" % basepath
os.mkdir(basepath)

if options.country:
  cmd_opts = '--parse_country %s' % options.country
else:
  cmd_opts = '--parse_world true'

if options.output_prefix_index:
  cmd_opts += ' --output_prefix_index true'
else:
  cmd_opts += ' --output_prefix_index false'
  
cmd = './sbt "indexer/run-main com.foursquare.twofishes.importers.geonames.GeonamesParser %s --hfile_basepath %s"' % (cmd_opts, basepath)
print(cmd)

if not options.dry_run:
  os.system(cmd)
  os.unlink("latest")
  os.symlink(basepath, "latest")


