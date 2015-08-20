#!/usr/bin/python

import os
import sys
import locale
from optparse import OptionParser

usage = "usage: %prog [options] hfile_directory"
parser = OptionParser(usage = usage)
parser.add_option("--host", dest="host",  default="0.0.0.0", type='string',
  help="host")
parser.add_option("-p", "--port", dest="port",  default=8080, type='int',
  help="port")
parser.add_option("--warmup", dest="warmup",  default=False, action='store_true',
  help="warmup service, increases startup time")
parser.add_option("--preload", dest="preload",  default=False, action='store_true',
  help="preload index to prevent coldstart, increases startup time")
parser.add_option("--nopreload", dest="preload",  default=False, action='store_false',
  help="don't preload index to prevent coldstart, decrease startup time, increases intial latency")
parser.add_option("-r", "--rebel", dest="rebel",  default=False, action='store_true',
  help="rebel")
parser.add_option("-c", "--console", dest="console",  default=False, action='store_true',
  help="console, not server")
parser.add_option("--hotfix_basepath", dest="hotfix", default="", type='string',
  help="path to hotfixes")
parser.add_option("--enable_private_endpoints", dest="enablePrivate", default=False, action='store_true',
  help="enable private endpoints on server")
parser.add_option("--vm_map_count", dest="vm_map_count", type='int', help="port")


(options, args) = parser.parse_args()

if len(args) != 1:
  parser.print_usage()
  sys.exit(1)

if (locale.getdefaultlocale()[1] != 'UTF-8' and
    locale.getdefaultlocale()[1] != 'UTF8'):
  print "locale is not UTF-8, unsure if this will work"
  print "see: http://perlgeek.de/en/article/set-up-a-clean-utf8-environment for details"
  sys.exit(1)

basepath = os.path.abspath(args[0])

sbt = './sbt'
if options.rebel:
  sbt = './sbt-rebel'

args = ' --preload %s --warmup %s --enable_private_endpoints %s ' % (options.preload, options.warmup, options.enablePrivate)
if (len(options.hotfix) > 0):
  args += '--hotfix_basepath %s ' % options.hotfix

if options.vm_map_count:
  args += '--vm_map_count ' + str(options.vm_map_count)

if options.console:
  target = 'console'
else:
  target = 'run-main'

cmd = '%s "server/%s com.foursquare.twofishes.GeocodeFinagleServer %s --host %s --port %d --hfile_basepath %s"' % (sbt, target, args, options.host, options.port, basepath)

print(cmd)
os.system(cmd)

