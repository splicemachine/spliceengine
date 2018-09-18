#!/usr/bin/python
import re
import sys
import os

file_dir = "/opt/mapr/hadoop/hadoop-2.7.0/etc/hadoop/"
fname = file_dir + "yarn-site.xml.new"
fname_orig = file_dir + "yarn-site.xml.orig"
yarn_file = file_dir + "yarn-site.xml"

#Make a copy first.
cmd = "cp %s %s"%(yarn_file,fname_orig)
os.system(cmd)

cp_file = "yes"

fout = open(fname, 'w')
with open(yarn_file, "r") as fin:
   for line in fin:
      if re.search(r'yarn.scheduler|yarn.nodemanager|spark.authenticate', line, re.M|re.I):
              cp_file = "no" #already configure. Do not copy.
              break
      if re.search(r'</configuration>', line, re.M|re.I):
              fout.write("  <!-- yarn resource settings are installation specific -->\n")
              fout.write("  <property><name>yarn.scheduler.minimum-allocation-mb</name><value>1024</value></property>\n")
              fout.write("  <property><name>yarn.scheduler.increment-allocation-mb</name><value>512</value></property>\n")
              fout.write("  <property><name>yarn.scheduler.maximum-allocation-mb</name><value>30720</value></property>\n")
              fout.write("  <property><name>yarn.nodemanager.resource.memory-mb</name><value>30720</value></property>\n")
              fout.write("  <property><name>yarn.scheduler.minimum-allocation-vcores</name><value>1</value></property>\n")
              fout.write("  <property><name>yarn.scheduler.increment-allocation-vcores</name><value>1</value></property>\n")
              fout.write("  <property><name>yarn.scheduler.maximum-allocation-vcores</name><value>19</value></property>\n")
              fout.write("  <property><name>yarn.nodemanager.resource.cpu-vcores</name><value>19</value></property>\n")
              fout.write("  <property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle,mapr_direct_shuffle,spark_shuffle</value></property>\n")
              fout.write("  <property><name>yarn.nodemanager.aux-services.mapreduce_shuffle.class</name><value>org.apache.hadoop.mapred.ShuffleHandler</value></property>\n") 
              fout.write("  <property><name>yarn.nodemanager.aux-services.spark_shuffle.class</name><value>org.apache.spark.network.yarn.YarnShuffleService</value></property>\n")
              fout.write("  <property><name>yarn.nodemanager.log-dirs</name><value>/opt/mapr/hadoop/hadoop-2.7.0/logs/userlogs</value></property>\n")
              fout.write("  <property><name>spark.authenticate</name><value>false</value></property>\n")
              fout.write("</configuration>\n")
      else:
              fout.write(line)

fout.close()

#cmd = "cp fname yarn_file"
cmd = "cp %s %s"%(fname,yarn_file)

if cp_file == "yes":
   os.system(cmd)





