#!/usr/bin/env python

import tarfile
import urllib
import re
import StringIO
import os

basedir = 'data/downloaded'

flickr_shapes_file_name = os.path.join(basedir, 'flickr_shapes_public_dataset_2.0.tar.gz')

try:
  open(flickr_shapes_file_name)
except IOError as e:
  print 'Downloading Flickr Shapes File to %s' % flickr_shapes_file_name
  urllib.urlretrieve ('http://www.flickr.com/services/shapefiles/2.0/', flickr_shapes_file_name)

print 'done downloading'

old_tar = tarfile.open(flickr_shapes_file_name)

for file_info in old_tar:
  print 'Processing %s' % file_info.name
  old_data = old_tar.extractfile(file_info.name).read()

  p = re.compile(',(\s+})')
  new_data = p.sub('\\1', old_data)


  print 'Writing updated %s' % file_info.name

  new_file = open(os.path.join(basedir, file_info.name), "w")
  new_file.write(new_data)
  new_file.close()
