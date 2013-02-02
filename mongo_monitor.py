#!/usr/bin/env python
# -*- coding: utf-8 -*-
# vim: set sts=4 sw=4 encoding=utf-8

from pymongo import Connection
from pymongo import ReadPreference
from optparse import OptionParser,OptionGroup
import json
import urllib2
import csv
import sys
from itertools import chain, count, izip_longest
from pprint import pprint

options = {}

def collectKeys(parent,json_array,keys):  
  if keys is None: 
    keys = []
  for json_key in json_array:

    if json_key == ".":
      continue

    if type(json_array[json_key]) == dict:
      keys = collectKeys(parent+json_key+".",json_array[json_key],keys)
    else:
      keys.append(str(parent+json_key))
  return keys

def printData(keys,json_array):
  vals = []

  for key in keys:
    parts = key.split('.')
    parts.reverse()
    
    cur_val = json_array
    
    while len(parts) > 0:
      cur_val = cur_val[parts.pop()]  

    vals.append(str(cur_val))

  print ",".join(vals)

def jsonFrom(url):
  req = urllib2.Request('http://'+options.dbhost+':'+options.dbrestport+'/'+url)
  opener = urllib2.build_opener()
  f = opener.open(req)
    
  return json.loads(f.read())

def getMongoTop():  
  json_array = jsonFrom('top')

  db_top = json_array["totals"][options.dbname]
    
  keys = collectKeys("",db_top,None)
  print ",".join(keys)
  printData(keys,db_top)  

def getMongoStatus():
  json_array = jsonFrom('_status')

  db_status = json_array['serverStatus']

  keys = collectKeys("",db_status,None)
  print ",".join(keys)
  printData(keys,db_status)

def getConnectionRates():
  #Will write it`s state to file, then will return difference using timestamps.
  #If there is no file, then it will execute, wait for a 3 seconds, and calculate rate
  print "test"  
if __name__ == "__main__":
  from sys import argv
  from os import environ   

  usage = "usage: %prog [options] arg"
  parser = OptionParser(usage)
  parser.add_option("-H","--host", dest="dbhost",
                  help="MongoDB hostname (localhost)", metavar="HOSTNAME",default="localhost")
  parser.add_option("-p","--port", dest="dbport",
                  help="MongoDB port (27017)", metavar="PORT",default="27017")
  parser.add_option("-r","--rest-port", dest="dbrestport",
                  help="MongoDB REST API port (28017) ", metavar="PORT",default="28017")
  parser.add_option("-c","--collection", dest="dbname",
                  help="MongoDB collection (admin)", metavar="COLLECTION",default="admin")
  parser.add_option("-a","--action", dest="action",type="choice",default="collection_rate",choices=["collection_top","server_status","collection_rate"],
                  help="Action to run (by default it`s server_status)")
  group = OptionGroup(parser, "Rates Options")
  parser.add_option("-f","--file", dest="rate_file",metavar="FILE",
                  help="Action to run (by default it`s server_status)")
  parser.add_option_group(group)
  (options, args) = parser.parse_args()    

  if (options.action == 'collection_rate'):
    getConnectionRates()
  elif (options.action == 'collection_top'):
    getMongoTop()
  elif (options.action == 'server_status'):
    getMongoStatus()