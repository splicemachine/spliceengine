#!/bin/bash

#logon to master server and execute the script.
#sudo ./installSplicemachineUnSecure.sh centos https://s3.amazonaws.com/splice-releases/2.7.0.1835/cluster/installer/mapr6.0.0 2.7.0.1835.mapr6.0.0.p0.146 mapr6.0.0 

. ./splice.ini
splice=SPLICEMACHINE-${splice_release}
splice_url=$1
splice_tar=${splice}.tar.gz
splicePackage=${splice_url}/${splice_tar}
mapr_platform=$2
export MYVER=1.1.8-${splice}

all_nodes=( $mapr_server_dns ${client_nodes[@]} )
dir_decoupled="/opt/splice/decoupled-install"
url_decoupled="raw.githubusercontent.com/splicemachine/spliceengine/master/assembly"

eval $(ssh-agent -s)
ssh-add /home/${userID}/splice-dcos.pem

startStopWarden () {
   node=$1
   echo -e "\n$2 mapr-warden at $node"
   ssh -o StrictHostKeyChecking=no -o CheckHostIP=no -A -t ${userID}@${node} "sudo service mapr-warden $2"
}
#Start/Stop hregions
startStopRegion () {
   node=$1
   echo -e "\n$2 splice-regionserver at $node"
   ssh -o StrictHostKeyChecking=no -o CheckHostIP=no -A -t ${userID}@${node} "sudo service splice-regionserver $2"
}

ps aux | grep HMaster > /dev/null
if [ $? -eq 0 ]; then
  echo -e "\nStop splice-master"
  sudo service splice-master stop

  #Stop HRegionServer:
  for i in "${client_nodes[@]}"
  do
     startStopRegion $i "stop"
  done
fi

install_config () {
   node=$1
   echo -e "Configuring  $node ......"
   #Remove packages in case it is re-installation.
   ssh -o StrictHostKeyChecking=no -o CheckHostIP=no -A -t ${userID}@${node} "cd /opt/splice; sudo unlink default; sudo rm -rf ${splice}*; cd /opt/mapr/hbase; sudo rm -rf hbase-${MYVER}; cd -"
   ssh -o StrictHostKeyChecking=no -o CheckHostIP=no -A -t ${userID}@${node} "sudo chmod 666 /opt/mapr/hbase/hbaseversion; sudo echo $MYVER > /opt/mapr/hbase/hbaseversion"
   if [[ $splice_url != "no_url" ]]; then
      ssh -o StrictHostKeyChecking=no -o CheckHostIP=no -A -t ${userID}@${node} "sudo wget --directory-prefix=/opt/splice $splicePackage"
   fi
   ssh -o StrictHostKeyChecking=no -o CheckHostIP=no -A -t ${userID}@${node} "sudo tar -xf /opt/splice/${splice_tar} --directory /opt/splice; \
                                                                        sudo mkdir -p ${dir_decoupled}/scripts; \
                                                                        sudo mkdir -p ${dir_decoupled}/conf; \
                                                                        sudo wget --directory-prefix=${dir_decoupled}/scripts https://${url_decoupled}/${mapr_platform}/src/main/resources/examples/scripts/splice-init-script; \
                                                                        sudo wget --directory-prefix=${dir_decoupled}/conf https://${url_decoupled}/${mapr_platform}/src/main/resources/examples/conf/mapr-splice-hbase-config.sh; \
                                                                        sudo wget --directory-prefix=${dir_decoupled}/scripts https://${url_decoupled}/common/src/main/resources/scripts/install-splice-symlinks.sh; \
                                                                        sudo chmod +x ${dir_decoupled}/scripts/splice-init-script; \
                                                                        sudo chmod +x ${dir_decoupled}/scripts/install-splice-symlinks.sh; \
                                                                        sudo chmod +x ${dir_decoupled}/conf/mapr-splice-hbase-config.sh; \
                                                                        sudo chown -R mapr:mapr /opt/splice; \
                                                                        sudo ln -s /opt/splice/${splice} /opt/splice/default; \
                                                                        sudo bash ${dir_decoupled}/scripts/install-splice-symlinks.sh; \
                                                                        sudo sed -i.orig -e 's/env -i //' ${dir_decoupled}/scripts/splice-init-script; \
                                                                        sudo cp -pr /opt/mapr/hbase/hbase-1.1.8{,-${splice}}; \
                                                                        sudo rm -f /opt/mapr/hbase/hbase-1.1.8-${splice}/logs/hbase-*; \
                                                                        sudo cp /opt/splice/decoupled-install/conf/mapr-splice-hbase-config.sh /opt/mapr/conf; \
                                                                        sudo sed -i.SPLICE-DEFAULT \"s#^\(HBASE_VER=\).*#\1${MYVER}#\" /opt/mapr/conf/mapr-splice-hbase-config.sh; \
                                                                        sudo sed -i.MAPR-DEFAULT '/HBASE_VOLUME/ s/mapr\.hbase/splice.hbase/g' /opt/mapr/hbase/hbase-${MYVER}/bin/hbase-daemon.sh; \
                                                                        sudo sed -i.MAPR-DEFAULT 's#conf/mapr-hbase-config\.sh#conf/mapr-splice-hbase-config.sh#g' /opt/mapr/hbase/hbase-${MYVER}/bin/hbase-config.sh; \
                                                                        sudo cp /opt/splice/decoupled-install/scripts/splice-init-script /etc/rc.d/init.d; \
                                                                        sudo chmod 755 /etc/rc.d/init.d/splice-init-script; \
                                                                        sudo sed -i \"s#^\(hbase_ver=\).*#\1${MYVER}#\" /etc/rc.d/init.d/splice-init-script; \
                                                                        sudo patch -b -p0 /opt/mapr/hbase/hbase-${MYVER}/conf/hbase-env.sh < /opt/splice/default/conf/hbase-env.sh.secure.patch; \
                                                                        sudo sed -i.DEFAULT "s/hbase-1.1.1/hbase-${MYVER}/g" /opt/mapr/hbase/hbase-${MYVER}/conf/hbase-env.sh; \
                                                                        sudo sed -e 's/SPARKHISTORYSERVER/hostname:18088/' -i /opt/mapr/hbase/hbase-${MYVER}/conf/hbase-env.sh; \
                                                                        sudo patch -b -p0 /opt/mapr/hbase/hbase-${MYVER}/conf/hbase-site.xml /opt/splice/default/conf/hbase-site.xml.secure.patch; \
                                                                        sudo sed -e '/<name>hbase.regionserver.handler.count<\/name>/{n;s/<value>.*<\/value>/<value>400<\/value>/}' -i.orig /opt/mapr/hbase/hbase-${MYVER}/conf/hbase-site.xml; \
                                                                        sudo sed -e '/<name>hbase.rootdir<\/name>/{n;s@<value>.*</value>@<value>maprfs:///splice-hbase</value>@}' -i /opt/mapr/hbase/hbase-${MYVER}/conf/hbase-site.xml; \
                                                                        sudo wget  --directory-prefix=/opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/yarn/ https://archive.apache.org/dist/spark/spark-2.1.1/spark-2.1.1-bin-hadoop2.7.tgz; \
                                                                        sudo cp /opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/tools/lib/commons-lang3-3.3.2.jar /opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/common/lib/; \
                                                                        sudo cp /opt/mapr/spark/spark-2.1.0/yarn/spark-2.1.0-mapr-1710-yarn-shuffle.jar /opt/mapr/hadoop/hadoop-2.7.0/share/hadoop/yarn/lib/; \
                                                                        sudo /home/${userID}/config-hbase-site.py $MYVER; \
                                                                        sudo /home/${userID}/config-yarn-site.py"

#Create the Splice Machine Event Log Directory in hdfs
ssh -o StrictHostKeyChecking=no -o CheckHostIP=no -A -t ${userID}@${node} << EOF
   sudo su - mapr
   maprlogin password <<< "mapr"
   hadoop fs -mkdir -p maprfs:///splice-hbase
   hadoop fs -mkdir -p maprfs:///user/splice/history
   hadoop fs -chown -R mapr:mapr maprfs:///user/splice/history
   hadoop fs -chmod 777 maprfs:///user/splice/history
EOF
}

for i in "${all_nodes[@]}"
do
        install_config $i
done

#Install splice init script on SPLICE HBASE REGIONSERVERS only
install_splice_init () {
   node=$1
   scp -o StrictHostKeyChecking=no -o CheckHostIP=no /opt/splice/decoupled-install/scripts/splice-init-script ${userID}@${node}:/tmp  #required by 3.10, copy to region servers.
   ssh -o StrictHostKeyChecking=no -o CheckHostIP=no -A -t ${userID}@${node} "sudo mv /tmp/splice-init-script /etc/rc.d/init.d; \
	                                                                                     sudo ln -sf /etc/rc.d/init.d/splice-init-script /etc/rc.d/init.d/splice-regionserver; \
											     sudo chkconfig --add splice-regionserver; sudo chkconfig --list splice-regionserver"
}
for i in "${client_nodes[@]}"
do
        install_splice_init $i
done

#Install splice init script on SPLICE HBASE MASTERS only
sudo ln -sf /etc/rc.d/init.d/splice-init-script /etc/rc.d/init.d/splice-master
sudo chkconfig --add splice-master 
sudo chkconfig --list splice-master

#Restart Cluster Services
#1. Start mapr warden on Master. 2. Start warden on all region serves. 3. start hmaster. 4. Start hregions

#Stop service:
#1). Stop hregions:
for i in "${client_nodes[@]}"
do
        startStopRegion $i "stop"
done

#2). Stop hmaster
echo -e "\nStop splice-master"
sudo service splice-master stop

#3). Stop mapr-warden on all region servers.
for i in "${client_nodes[@]}"
do
        startStopWarden $i "stop"
done

#4). Stop mapr warden on Master
echo -e "\nStop mapr-zookeeper, mapr-warden"
sudo service mapr-warden stop
sudo service mapr-zookeeper stop

#Start services
#1). Start mapr-warden on Master.
echo -e "\nStarting mapr-zookeeper, mapr-warden"
sudo service mapr-zookeeper start
sudo service mapr-warden start

#2). Start warden on all regions
for i in "${client_nodes[@]}"
do
        startStopWarden $i "start"
done

#3). Start hmaster
echo -e "\nStarting splice-master"
sudo service splice-master start

#4). Start hregions
for i in "${client_nodes[@]}"
do
        startStopRegion $i "start"
done

#Tighten ephemeral ports range for HBase
sudo /opt/splice/default/scripts/install-sysctl-conf.sh

sudo sed -e 's/maxClientCnxns=.*/maxClientCnxns=0/g' -i.orig /opt/mapr/zookeeper/zookeeper-3.4.5/conf/zoo.cfg
sudo sed '/^maxSessionTimeout=/d' -i.orig /opt/mapr/zookeeper/zookeeper-3.4.5/conf/zoo.cfg
sudo chmod 777 /opt/mapr/zookeeper/zookeeper-3.4.5/conf/zoo.cfg
sudo -iu mapr echo maxSessionTimeout=120000 >> /opt/mapr/zookeeper/zookeeper-3.4.5/conf/zoo.cfg

