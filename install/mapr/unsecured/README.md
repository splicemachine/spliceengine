# Splice Machine Installation and Configuration

installSplice.sh: Script to install Splicemachine on top of MapR6 with security disabled.

splice.ini:       Configuration file for installation. 

## Note

1. The sample splice.ini has one master server, two client nodes. You may add more nodes. Construct array, client_nodes for all client nodes.

2. Assuem you use the user ID, centos to install. "centos" user should have sudo privilege. You have a home directory /home/centos at all nodes. Change centos to the ID of your choice.

3. SpliceMachine package will be downloaded from https://s3.amazonaws.com/splice-releases by default if local_path is undefined.

splice.ini:

````
userID=centos
#splice_release: SpliceMachine package version.
splice_release=2.7.0.1835.mapr6.0.0.p0.146

#local_path: The path to the tar ball at your local drive. if defined, tar ball will be upload
#            to MapR server from local.
local_path=

#mapr_server_dns: MapR server hostname
mapr_server_dns=ec2-52-23-199-220.compute-1.amazonaws.com

#MapR client nodes:
node1=ec2-34-227-193-89.compute-1.amazonaws.com
node2=ec2-18-208-188-58.compute-1.amazonaws.com

#Construct array based on list of client nodes.
client_nodes=( $node1 $node2 )
````
