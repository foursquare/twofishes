# Copyright 2011 Foursquare Labs Inc. All Rights Reserved.

import math
import string
import traceback
import unicodedata
import re

def featureComparator(f):
  return -1*getLevelFromCode(f['feature_code'])

def getLevelFromCode(code):
  if code.startswith('PCL'):
    return 0
  elif code == 'ADM1':
    return 1
  elif code == 'ADM2':
    return 2
  elif code == 'ADM3':
    return 3
  elif code == 'ADM4':
    return 4
  else:
    return 100


def tokenize(s):
  return s.split(' ')

intab = string.punctuation 
outtab = u''.join([u" " for a in string.punctuation])
trantab = dict((ord(a), b) for a, b in zip(intab, outtab))
def normalize(s):
  s = unicode.lower(s)
  # replace ' with ""
  s = s.replace("'", "")
  s = s.replace("\u2018", "")
  s = s.replace("\u2019", "")
  # replace all other punctuation with spaces
  s = s.translate(trantab)
  # replace multiple spaces with one space
  s = re.sub(' +', ' ', s)
  # clear spaces at start/end
  return s.strip()

def deaccent(unistr):
  try:
    return "".join(aChar 
        for aChar in unicodedata.normalize("NFD", unistr) 
        if "COMBINING" not in unicodedata.name(aChar))
  except:
    return unistr

def distance_km(lat1, lon1, lat2, lon2):
   '''
   Given a set of geo coordinates (in degrees) it will return the distance in km
   '''

   R = 6371;
   lat1 = math.radians(lat1)
   lat2 = math.radians(lat2)
   lon1 = math.radians(lon1)
   lon2 = math.radians(lon2)
   c = math.sin(lat1)*math.sin(lat2) + math.cos(lat1)*math.cos(lat2) * math.cos(lon2-lon1)
   if c > 1 and c < 1.000000001:
     return 0
   else:
     return math.acos(c) * R
