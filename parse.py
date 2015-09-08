#!/usr/bin/python

import datetime
import os
import os.path
import socket
import sys
from optparse import OptionParser
import subprocess

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
parser.add_option("-s", "--output_s2_covering_index", dest="output_s2_covering_index",  action="store_true", default=False,
  help="output s2 covering index (optional)")
parser.add_option("-i", "--output_s2_interior_index", dest="output_s2_interior_index",  action="store_true", default=False,
  help="output s2 interior covering index (optional)")
parser.add_option("-n", "--dry_run", dest="dry_run",  action="store_true", default=False)
parser.add_option("--reload", dest="reload_data",  action="store_true", default=True, help="reload data into mongo")
parser.add_option("--noreload", dest="reload_data",  action="store_false", help="don't reload data into mongo")
parser.add_option("--yes-i-am-sure", dest="yes_i_am_sure",  action="store_true", default=False, help="skip asking about reloading")
parser.add_option("-g", "--geonamesonly", dest="geonamesonly", action="store_true",  default=False,
  help="geonames is the canonical gazetteer and gets id namespace 0")

(options, args) = parser.parse_args()

mongo_version_str = subprocess.Popen('mongod --version', stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True).stdout.readline().split(' v')[-1]
mongo_version = mongo_version_str.split('.')
if mongo_version[0] < 2 or mongo_version[1] < 4:
  print 'need at least mongo 2.4, you have: %s' % mongo_version_str
  sys.exit(1)

user_specified_basepath = False
basepath = ''
if len(args) != 0:
  if not args[0].startswith("-"):
    user_specified_basepath = True
    basepath = args[0]
    args = args[1:]

now_str = str(datetime.datetime.now()).replace(' ', '-').replace(':', '-')
if not basepath:
  basepath = os.path.join('indexes', basepath, now_str)

if not os.path.exists('indexes'):
  os.mkdir('indexes')
print "outputting index to %s" % basepath
if not os.path.exists(basepath):
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
passBoolOpt('output_s2_covering_index', options.output_s2_covering_index)
passBoolOpt('output_s2_interior_index', options.output_s2_interior_index)
passBoolOpt('output_prefix_index', options.output_prefix_index)
passBoolOpt('reload_data', options.reload_data)

jvm_args = []
if options.geonamesonly:
  jvm_args.append("-DgeonameidNamespace=0")

if options.reload_data and not options.yes_i_am_sure:
  if raw_input('Are you suuuuuure you want to drop your mongo data? Type "yes" to continue: ') != 'yes':
    print "Bailing."
    print
    print "re-run with --noreload if you want to keep your mongo data around instead of rebuilding it"
    sys.exit(1)

cmd = './sbt %s "indexer/run-main com.foursquare.twofishes.importers.geonames.GeonamesParser %s --hfile_basepath %s %s"' % (' '.join(jvm_args), cmd_opts, basepath, ' '.join(args))
print(cmd)

version_file = open(os.path.join(basepath, 'index-gen-info-%s' % now_str), 'w')
version_file.write('Command: %s\n' % ' '.join(sys.argv))
version_file.write('User: %s\n' % os.getenv('USER'))
version_file.write('Date: %s\n' % now_str)
version_file.write('Host: %s\n' % socket.gethostname())
version_file.close()

if not options.dry_run:
  os.system(cmd)
  if not user_specified_basepath:
    if os.path.exists("latest"):
      os.unlink("latest")
    os.symlink(basepath, "latest")


