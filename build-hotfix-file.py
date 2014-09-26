#!/usr/bin/python

import os
import sys
from optparse import OptionParser

usage = "usage: %prog output_file"
parser = OptionParser(usage = usage)
(options, args) = parser.parse_args()

if len(args) != 1:
  parser.print_usage()
  sys.exit(1)

file = args[0]

cmd = './sbt "server/run-main com.foursquare.twofishes.JsonHotfixFileBuilder %s"' % file

print(cmd)
os.system(cmd)