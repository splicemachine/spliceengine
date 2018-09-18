#!/usr/bin/python
import re
import sys
import os

MYVER = sys.argv[1]
file_dir = "/opt/mapr/hbase/hbase-" + MYVER + "/conf"
fname = file_dir + "/hbase-site.xml.new"
fname_orig = file_dir + "/hbase-site.xml.orig"
hbase_file = file_dir + "/hbase-site.xml"

#Make a copy first.
cmd = "cp %s %s"%(hbase_file,fname_orig)
os.system(cmd)

cp_file = "yes"
#hbase-site.yaml
fout = open(fname, 'w')
with open(hbase_file, "r") as fin:
   for line in fin:
      if re.search(r'hbase.master.port|hbase.master.info.port|hbase.regionserver.port|hbase.regionserver.info.port|hbase.status.multicast.address.port|zookeeper.znode.parent', line, re.M|re.I):
              cp_file = "no" #already configure. Do not copy.
              break
      if re.search(r'spark.authenticate', line, re.M|re.I):
              fout.write("  <property><name>spark.authenticate</name><value>false</value></property>\n")
      if re.search(r'</configuration>', line, re.M|re.I):
              fout.write("  <property><name>hbase.master.port</name><value>60001</value></property>\n")
              fout.write("  <property><name>hbase.master.info.port</name><value>60011</value></property>\n")
              fout.write("  <property><name>hbase.regionserver.port</name><value>60021</value></property>\n")
              fout.write("  <property><name>hbase.regionserver.info.port</name><value>60031</value></property>\n")
              fout.write("  <property><name>hbase.status.multicast.address.port</name><value>60101</value></property>\n")
              fout.write("  <property><name>zookeeper.znode.parent</name><value>/splice-hbase</value></property>\n")
              fout.write("</configuration>\n")
      else:
              line = line.replace('maprdb', 'hbase')
              fout.write(line)

fout.close()

#cmd = "cp fname hbase_file"
cmd = "cp %s %s"%(fname,hbase_file)

if cp_file == "yes":
   os.system(cmd)

#hbase-env.sh
fname = file_dir + "/hbase-env.sh.new"
fname_orig = file_dir + "/hbase-env.sh.orig"
hbase_file = file_dir + "/hbase-env.sh"

#Make a copy first.
cmd = "cp %s %s"%(hbase_file,fname_orig)
os.system(cmd)

lib_dir = "/opt/mapr/hbase/hbase-" + MYVER + "/lib/*:/opt/splice/default/lib/*:/opt/splice/default/lib/*"

spark_authenticate="SPLICE_HBASE_MASTER_OPTS=\"$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.authenticate=false\"\n"
spark_authenticate_enableSaslEncryption="SPLICE_HBASE_MASTER_OPTS=\"$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.authenticate.enableSaslEncryption=false\"\n"
splice_spark_ssl_akka_enabled="SPLICE_HBASE_MASTER_OPTS=\"$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.ssl.akka.enabled=false\"\n"
splice_spark_ssl_fs_enabled="SPLICE_HBASE_MASTER_OPTS=\"$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.ssl.fs.enabled=false\"\n"

yarn_user="SPLICE_HBASE_MASTER_OPTS=\"$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.yarn.user=mapr\"\n"
hadoop_user="SPLICE_HBASE_MASTER_OPTS=\"$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.yarn.appMasterEnv.HADOOP_USER=mapr\"\n"
hbase_user ="SPLICE_HBASE_MASTER_OPTS=\"$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.yarn.appMasterEnv.HBASE_USER=mapr\"\n"
lib_driver="SPLICE_HBASE_MASTER_OPTS=\"$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.driver.extraClassPath=" + file_dir + ":" + lib_dir + '"\n'
lib_executor="SPLICE_HBASE_MASTER_OPTS=\"$SPLICE_HBASE_MASTER_OPTS -Dsplice.spark.executor.extraClassPath=" + file_dir + ":" + lib_dir + '"\n'
fout = open(fname, 'w')
with open(hbase_file, "r") as fin:
   for line in fin:
      if re.search(r'Dsplice.spark.driver.extraClassPath', line, re.M|re.I):
              fout.write(lib_driver)
      elif re.search(r'Dsplice.spark.executor.extraClassPath', line, re.M|re.I):
              fout.write(lib_executor)
              fout.write(hadoop_user)
              fout.write(hbase_user)
              fout.write(yarn_user)
      elif re.search(r'Dsplice.spark.authenticate', line, re.M|re.I):
              fout.write(spark_authenticate)
      elif re.search(r'spark.authenticate.enableSaslEncryption', line, re.M|re.I):
              fout.write(spark_authenticate_enableSaslEncryption)
      elif re.search(r'Dsplice.spark.ssl.akka.enabled', line, re.M|re.I):
              fout.write(splice_spark_ssl_akka_enabled)
      elif re.search(r'Dsplice.spark.ssl.fs.enabled', line, re.M|re.I):
              fout.write(splice_spark_ssl_fs_enabled)
      else:
              fout.write(line)

cmd = "cp %s %s"%(fname,hbase_file)
os.system(cmd)
