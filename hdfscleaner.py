#!/usr/bin/env python
import argparse
import logging
import re
from getpass import getuser
from requests import get
from subprocess import check_output
from xml.dom import minidom
from os.path import isfile

def get_jt_address():
   logger = logging.getLogger('hdfscleaner.get_jt_address')
   mapred_conf = '/etc/hadoop/conf/mapred-site.xml'
   if not isfile(mapred_conf):
     raise Exception('Mapreduce configuration file does not exist, are you sure this is a Hadoop client installed machine?')
   mapred_conf_parsed = minidom.parse(mapred_conf)
   properties = mapred_conf_parsed.getElementsByTagName("property")
   for property in properties:
      name = property.getElementsByTagName("name")[0]
      value = property.getElementsByTagName("value")[0]
      if name.firstChild.data == 'mapred.job.tracker':
         logger.debug("name:%s, value:%s" % (name.firstChild.data, value.firstChild.data.split(':')[0]))
         return value.firstChild.data.split(':')[0]
   raise Exception('Cannot find JT name entry from the config, HA enabled JT?')
      
def get_jt_identifier(jt_address):
   logger = logging.getLogger('hdfscleaner.get_jt_identifer')
   page = get('http://' + jt_address + ':50030/jobtracker.jsp')
   result = re.search('<b>Identifier:</b> (.*)<br>', page.content)
   logger.debug("JT Identifier: '%s'" % result.group(1))
   return result.group(1)

def get_target_dirs(jt_identifier):
   logger = logging.getLogger('hdfscleaner.get_target_dirs')
   staging_raw = check_output('hdfs dfs -ls /user/*/.staging'.split())
   staging_dirs = re.findall(r'^.*/user/([-\w]+)/.staging/(\w+)\s*$',staging_raw, re.M)
   if not staging_dirs:
      raise Exception('Nothing exists in .staging, is JT running still?')
   logger.debug("Staging dir count: '%s'" % len(staging_dirs))
   dirs_to_filter = [x for x in staging_dirs if x[1].startswith("job_" + jt_identifier)]
   logger.debug("Filtered dir count: '%s'" % len(dirs_to_filter))
   filtered_staging_dirs  = set(staging_dirs) - set(dirs_to_filter)
   return filtered_staging_dirs

def remove_target_dirs(target_dirs, dry_run):
   logger = logging.getLogger('hdfscleaner.remove_target_dirs')
   for target_dir in target_dirs:
      cmd = "hdfs dfs -rm -r -f -skipTrash " + "/user/"+target_dir[0]+"/.staging/"+target_dir[1]
      if dry_run:
         logger.debug("Dry run: '%s'" % cmd)
      else:
         result = check_output(cmd, shell=True)
         logger.debug("Cmd Result: '%s'" % result.strip())

if __name__ == '__main__':
   parser = argparse.ArgumentParser(description='Clean up staled staging files from HDFS')
   parser.add_argument('-d', '--dry-run', help='Enable dry run mode', action='store_true')
   parser.add_argument('--debug', help='Enable Debug Logging', action='store_true')
   args = parser.parse_args()

   if args.debug:
      log_level = logging.DEBUG
   else:
      log_level = logging.INFO

   logging.basicConfig(level=log_level, format='%(asctime)s:%(levelname)s:%(name)s:%(message)s')
   logger = logging.getLogger('hdfscleaner.main')

   if args.dry_run:
      logger.info("Dry run mode enabled")
   if not getuser() == 'hdfs':
      raise Exception('Need to be executed as hdfs user e.g) sudo -u hdfs')

   logger.info("Clean up task started")
   jt_address = get_jt_address()
   jt_identifier = get_jt_identifier(jt_address)
   target_dirs = get_target_dirs(jt_identifier)
   remove_target_dirs(target_dirs,args.dry_run)
   logger.info("Clean up task finished")
