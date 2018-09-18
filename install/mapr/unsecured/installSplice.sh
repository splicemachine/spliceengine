#!/bin/bash

. ./splice.ini

nodes=( $mapr_server_dns ${client_nodes[@]} )
green='\e[1;32m%s\e[0m'

copyFiles () {
   node=$1
   ssh -o StrictHostKeyChecking=no -o CheckHostIP=no -A -t ${userID}@${node} "sudo mkdir -p /opt/splice/decoupled-install/scripts; sudo chown -R ${userID}:${userID} /opt/splice/decoupled-install/scripts"
   scp -o StrictHostKeyChecking=no -o CheckHostIP=no install-splice-symlinks.sh ${userID}@${node}:/opt/splice/decoupled-install/scripts
   scp -o StrictHostKeyChecking=no -o CheckHostIP=no config-hbase-site.py ${userID}@${node}:/home/${userID}
   scp -o StrictHostKeyChecking=no -o CheckHostIP=no config-yarn-site.py ${userID}@${node}:/home/${userID}
}
for i in "${nodes[@]}"
do
        copyFiles $i
done

echo -e "\nInstalling and configuring spliceDB ......"
read t1 t2 t3 t4 t5 t6 t7 t9 <<<$(IFS="."; echo $splice_release)
splice_version=${t1}.${t2}.${t3}.${t4}
mapr_platform=${t5}.${t6}.${t7}
splice_tar=SPLICEMACHINE-${splice}.tar.gz

scp -o StrictHostKeyChecking=no -o CheckHostIP=no installSplicemachine.sh ${userID}@${mapr_server_dns}:/home/${userID}
scp -o StrictHostKeyChecking=no -o CheckHostIP=no splice.ini ${userID}@${mapr_server_dns}:/home/${userID}
if [ ${local_path} ]; then  #Copy tar ball from local drive to server.
   splice_url="no_url"
   echo -e "Uploading ${local_path}/${splice_tar} to MapR server ......"
   scp -i ${dir}/${key}.pem -o StrictHostKeyChecking=no -o CheckHostIP=no ${local_path}/${splice_tar} ${userID}@${mapr_server_dns}:/tmp
   ssh -i ${dir}/${key}.pem -o StrictHostKeyChecking=no -o CheckHostIP=no -A -t ${userID}@${mapr_server_dns} "sudo chown mapr:mapr /tmp/${splice_tar}; mv /tmp/${splice_tar} /opt/splice"
else
   splice_url="https://s3.amazonaws.com/splice-releases/${splice_version}/cluster/installer/${mapr_platform}"
fi
ssh -o StrictHostKeyChecking=no -o CheckHostIP=no -A -t ${userID}@${mapr_server_dns} "sudo /home/${userID}/installSplicemachine.sh ${splice_url} ${mapr_platform}"

printf "Finished deploying and configuring Splicemachine. Go to"
printf "\n\n$green\n\n" "https://${mapr_server_dns}:8443"
