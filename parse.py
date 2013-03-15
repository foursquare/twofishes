#!/usr/bin/python

import datetime
import os
import os.path
import socket
import sys
from optparse import OptionParser

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
parser.add_option("--reload", dest="reload_data",  action="store_true", default=True, help="reload data into mongo")
parser.add_option("--noreload", dest="reload_data",  action="store_false", help="don't reload data into mongo")



(options, args) = parser.parse_args()

basepath = '.'
if len(args) != 0:
  if not args[0].startswith("-"):
    basepath = args[0]
    args = args[1:]
now_str = str(datetime.datetime.now()).replace(' ', '-').replace(':', '-')
basepath = os.path.join(basepath, now_str)
print "outputting index to %s" % basepath
os.mkdir(basepath)

cmd_opts = ''

def passBoolOpt(opt, value):
  global cmd_opts
  if not opt.startswith('-'):
    opt = '--' + opt

  cmd_opts += ' %s %s' % (opt, str(value).lower())

if options.country:
  cmd_opts += ' --parse_country %s' % options.country
else:
  cmd_opts += ' --parse_world true'

passBoolOpt('output_revgeo_index', options.output_revgeo_index)
passBoolOpt('output_prefix_index', options.output_prefix_index)
passBoolOpt('reload_data', options.reload_data)

if options.reload_data:
  os.system("./init-database.sh")

cmd = './sbt "indexer/run-main com.foursquare.twofishes.importers.geonames.GeonamesParser %s --hfile_basepath %s %s"' % (cmd_opts, basepath, ' '.join(args))
print(cmd)

version_file = open(os.path.join(basepath, 'index-gen-info-%s' % now_str), 'w')
version_file.write('Command: %s\n' % ' '.join(sys.argv))
version_file.write('User: %s\n' % os.getenv('USER'))
version_file.write('Date: %s\n' % now_str)
version_file.write('Host: %s\n' % socket.gethostname())
version_file.close()

if not options.dry_run:
  os.system(cmd)
  if os.path.exists("latest"):
    os.unlink("latest")
  os.symlink(basepath, "latest")


