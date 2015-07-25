#Failure Modes Under Concurrent Load:

##Assumptions:

+ The ZooKeeper cluster is either entirely running, or entirely failed. There is a separate set of circumstances for when ZooKeeper is partially avaialble (i.e. when a quorum of ZooKeeper servers is available, but some nodes have failed), but the majority is handled by the ZooKeeper client transparently. When the entire cluster fails is a matter of our concern, however.
+ HDFS remains working correctly. Generally, this isn't true in most network partitions modes--if the RegionServer becomes partitioned, chances are the DataNode will too. However, we can't do much about the failure modes of HDFS, so we are going to assume the cluster is working.

##Definitions

**Client** = The JDBC client

**ControlNode** = The node to which a JDBC connection is established.

**RegionServer** = The node on which the Region of interest resides

**HMaster** = The node on which the HMaster process is currently running

**NameNode** = The node on which the HDFS NameNode is currently running

**Backup HMaster** = The node on which the backup HMaster is running

**Backup NameNode** = The node on which the backup NameNode is running

**Storage Node** = A node running an HDFS DataNode process. Usually the same as a RegionServer

##Network Topology

+ Each RegionServer must be network accessible to:
	- ZooKeeper
	- HMaster
	- NameNode
	- All Other RegionServers
+ HMaster must be network accessible to:
	- ZooKeeper
	- All RegionServers
+ NameNode must be network accessible to:
	- All Storage Nodes
+ Storage Nodes must be network accessible to:
	- All Other Storage Nodes
	- NameNode

##Network Failure Modes

###RegionServer Network failure modes
+ Region-To-Region partition = some, but not all, of the RegionServers in the cluster are network-inaccessible from one RegionServer 
+ Region-To-HMaster partition = the HMaster is network-inaccessible from one RegionServer
+ Region-To-ZooKeeper partition = the ZooKeeper cluster is network-inaccessible from one RegionServer
+ Region-To-NameNode partition = The NameNode is network-inaccessible from one RegionServer
+ Region-Total partition = all RegionServers in the cluster become network-inaccessible from one RegionServer, but the HMaster is accessible
+ HTotal partition = all RegionServers *and* the HMaster become network-inaccessible from one RegionServer.
+ Region-To-Storage partition = some,but not all, Storage Nodes becomes network-inaccessible from one RegionServer
+ Region-Total-Storage partition = All Storage nodes become network-inaccessible from one RegionServer

###HMaster Network failure modes
+ HMaster-To-ZooKeeper partition = ZooKeeper is network-inaccessible from the HMaster, but remains accessible from all RegionServers
+ HMaster-To-NameNode partition = the NameNode is network-inaccessible from the HMaster

###Client Failure modes
+ ControlNode partition = The ControlNode becomes network-inaccessible from the Client, but other RegionServers are available
+ Cluster partition = All RegionServers in the cluster become network-inaccessible from the Client

##Process Failure modes

###RegionServer process failure
+ Graceful shutdown = A RegionServer is gracefully shut down as part of normal operations
+ Process Death = The RegionServer process is killed, either through a Kill -9, or through some form of critical process state (such as an OutOfMemory error)
+ Zombie node = The Regionserver dies in such a way that the process is still running, and it remains network-accessible, but is unable to process any operations (e.g. total deadlock or GC Storm)
+ Latency Storm = The RegionServer remains alive, and serving requests, but the latency in serving requests violates SLA configurations

###HMaster process failure
+ Graceful shutdown = the active HMaster is shutdown as part of normal operations
+ Process death = The active HMaster process is killed, either through a Kill -9, or through some other form of catastrophic failure
+ Zombie node = the active HMaster is dead, but the process is still running, and it remains network-accessible, but is unable to process any operations (e.g. GC storm scenarios)
+ Latency Storm = The HMaster remains alive and serving requests, but the latency in serving requests violates SLA configurations.

###ControlNode process failure
+ All RegionServer process failure issues

###Client process failure
+ The JDBC client process gracefully disconnects
+ The JDBC client process fails without safely disconnecting from the ControlNode

##Server Failure modes
+ Graceful decommission = The server is gracefully shut down as part of normal operations
+ Abrupt server shutdown = The server is abruptly shut down, in a non-graceful way, in a way which is not considered part of normal operations

#Physical results of failures

##Network 

###RegionServer 
+ Region-To-Region partition: Here, queries which are directed against some regions which are owned by the destination RegionServer will throw errors internally until 
	- Network access is restored
	- Regions are moved off the destination RegionServer to other RegionServers in the cluster.
+ Region-To-HMaster partition: Here, the HBase client should automatically detect that it cannot connect to the HMaster, and shut itself down proactively. In the meantime, this will manifest itself as an inability to acquire timestamps in the transaction system until network access is restored or the RegionServer is shut down. Once shut down, this state becomes a Process failure
+ Region-To-ZooKeeper partition: Here, the RegionServer will eventually detect that ZooKeeper connectivity is missing, and shut itself down catastrophically, converting to a Process failure
+ Region-To-NameNode partition: The RegionServer will be unable to write records. This will manifest as IOExceptions in the WritePipeline until connectivity is restored
+ Region-Total partitions: All queries originating from the affected node, or directed to a Region within that node, will have errors thrown relating to the connectivity (e.g. SocketTimeout, ConnectTimeout, and so forth). This will occur until connectivity is restored or the process is shut down.
+ HTotal partition: This is a special form of the HMaster partition. Either timestamps will be unavailable, or network errors will result with respect to other nodes until the process shuts itself down directly, converting to a catastrophic process death scenario.
+ Region-To-Storage: This should be transparently handled by the HDFS client, as it will automatically direct writes to another node. It will manifest itself as poor write and read performance
+ Region-Total-Storage: This will manifest itself as errors thrown during write and reads, and will occur until connectivity to at least *R* Storage nodes and the NameNode are restored ( *R* is the 
replication factor of HDFS)
 

###HMaster
+ HMaster-To-ZooKeeper: The HMaster will automatically detect that it can no longer connect to ZooKeeper, and will shut itself down, converting to an HMaster process death scenario
+ HMaster-To-NameNode: This *should not* ( as far as I know) cause any significant difficulties, but it is possible that some Splice operations which write directly (like backup/restore) will not be successful until connectivity is restored

###Client
+ ControlNode partition: This will manifest itself as socket timeout, read timeout, or inability to connect in the JDBC client until either connectivity is restored or the client chooses a different node in the cluster
+ Cluster partition: No nodes are accessible to the client, so connection is impossible. This will manifest itself as client errors until connectivity to at least one node in the cluster is restored and the client connects to it

##Process Failure

###RegionServer
+ Graceful Shutdown: the HMaster will move regions to other RegionServers. NotServingRegionExceptions are thrown internally until all other servers can refresh internal caches and redirect traffic to new nodes.
+ Abrupt shutdown: Some socket timeout, IOExceptions, and failed writes will occur until either the process is restored, or until the HMaster detects the failure and moves the owned regions to new RegionServers (whichever comes first). If the process is restored within the session timeout, then the RegionServer will re-open its responsible regions, which will result in some NotServingRegionExceptions and some Socket timeouts until all regions are opened. If the process is not restored within the session timeout, the HMaster will move regions, and the newly started RegionServer process will be treated as a new RegionServer.
+ Zombie Node: This will manifest itself as no successful queries completing, and no errors thrown. It will appear to be "hung".
+ Latency Storm: Operations appear to be completing, but they are taking far longer than the SLA allows. This will manifest itself as very poor performance

###HMaster process failure
+ Graceful shutdown: A new leader election will occur and a backup HMaster will be chosen as the new HMaster. During this window of unavailability, timestamps should not be able to be generated by the HMaster, resulting in no new transactions being created.
+ Abrupt failure: No new Tables can be created, and existing Regions cannot split. Attempts to create a new Transaction timestamp will fail with SocketTimeout, ConnectTimeout, or other IO errors until a new HMaster can be elected.
+ Zombie Node: This will manifest as no errors, but CREATE TABLE and CREATE INDEX commands will never finish. Region splits will not occur, so regions will grow to a huge size as well. Most notably, because Timestamps are generated on the master, creating Transaction ids will never return. As a result, all new transactions will be stuck, resulting in non-existent performance
+ Latency Storm: This will manifest with no errors, but CREATE TABLE and CREATE INDEX commands will take a very long time to complete. As transaction ids are generated on the HMaster, this will also manifest as very poor performance when beginning new transactions and committing existing ones, resulting in poor performance.

###Client process failure
+ Graceful disconnect: This is automatically handled by the Derby client
+ Abrupt disconnect: Will need verification that queries are correctly paused by the Derby client.

#Expected Splice responses (Scott's opinion)

##Network

###RegionServer
+ Region-To-Region partition:
	- Any queries which are not directed to the affected server should continue to succeed (unless there are other failures in play preventing them).
	- Queries which involve data stored on the affected machine should return an appropriate error message to the client until the issue is resolved
	- Any automated monitoring (such as a Splice UI, Cloudera Manager/Ambari/Mapr Manager if possible) should display alerts about a potential connectivity issue
	- Future: Automatically causing that RegionServer to shut itself down through some kind of HMaster-relay kill mechanism? Would require some thought to do safely
+ Region-To-HMaster partition: Since this is converted to a process failure, we need only to deal with the window until the RegionServer shuts itself down
	- Return appropriate error message to client indicating that timestamps are unavailable, and thus new transactions cannot be created, and existing transactions will not be able to be committed
	- Gracefully disconnect all clients
	- Shut down the region server (or at least allow HBase to shut it down gracefully)
+ Region-To-ZooKeeper: Since this is converted to a process failure, we need only to deal with the window until the RegionServer shuts itself down
	- Return appropriate error message to the client.
	- Gracefully disconnect all clients 
	- Shut down the region server (or at least allow HBase to shut it down gracefully)
+ Region-To-NameNode: Return appropriate error message to the client until connectivity is restored. Do not allow any queries (as they may return incorrect answers if the NameNode is offline). Allow JDBC connections to connect to other nodes.
+ Region-Total: This is converted to a process failure, so it is proper to
	- Return an appropriate error message to the client
	- Gracefully disconnect all clients
	- Shut down the region server (or at least allow HBase to shut it down gracefully)
+ HTotal: This is also converted to a process failure, so 
	- Return an appropriate error message to the client
	- Gracefully disconnect all clients
	- Shut down the region server (or at least allow HBase to shut it down gracefully)
+ Region-Storage: Internal Error messages should be retried up to a configured number of times, as this should transparently recover itself 
+ Region-Total-Storage: This cannot be retried, since no storage nodes are available for write: 
	- An appropriate error message should be returned to the client until the situation is resolved
	- Any automated monitoring (such as a Splice UI, Cloudera Manager/Ambari/Mapr Manager if possible) should display alerts about a potential connectivity issue

###HMaster
+ HMaster-To-ZooKeeper: Failure to connect to ZooKeeper will be converted to an HMaster shutdown, so we should do the following:
	- Throw a retriable error on all attempts to create a timestamp (so as to prevent any accidental double-timestamp scenarios. These shouldn't occur anyway, but you can't be too safe)

###Client
+	ControlNode partition: The client should do two things:
	- Short term: return an error to the client and disconnect, allowing outside systems (such as commons-pool, or other middleware solutions) to retry the connection on a different node
	- Long term: automatically reconnect to a different node. When this happens, one of two scenarios are possible:
		+ Lost transaction: If a transaction is currently active, then that work is lost (it will eventually roll itself back). We may optionally choose to have the client automatically roll it back
		+ Recovered Transaction: The client informs the new ControlNode about the existing transaction, and if it was not timed out, the transaction information will be recovered automatically and no work is lost. This is mainly useful for OLAP operations--most OLTP operations would probably prefer the work is retried by the client than pay the added cost of distributing operation and transaction information.
+ Cluster partition: Nothing can be done: display an appropriate error message to the client

##Process Failure

###RegionServer
+ Graceful shutdown: 
	- No new connections are accepted
	- No further operations are allowed
	- Existing transactions are allowed to commit or roll themselves back
	- Clients are gracefully disconnected after their transaction completes
	- Natural HBase operations continue (e.g. regions are moved to other servers)
	- Ongoing operations from other clients are not disrupted--they still complete
+ Abrupt shutdown: There is a window of time where other RegionServers will assume that this server is still up, and so will attempt operations against it. These operations should:
	- timeout within a reasonable time
	- After timeout, automatically retry (up to a configurable limit) the operation against the same region, as the HMaster should have automatically begun moving regions as soon as the failure is detected.
	- If a certain amount of time has elapsed, or a configured number of retries has been exceeded, then the query should be failed with an appropriate error message so that the client can respond accordingingly.
+ Zombie Node: This is detected only by the failure to proceed with operations, so we should
	- timeout any internal operations against the zombie node, then
	- Inform the HMaster to move all Regions off that server, before
	- Retrying the internal oeprations against the newly located regions a configured number of times. If that number of times has been exceeded, then return an approparite error message to the client
+ Latency Storm: This is detected only by the timing out of network operations, so we should
	- timeout any internal operations against the affected node, then
	- Inform the master to move all Regions off the server
	- Inform the Server that he is behaving unnacceptably, so that it may shut itself down
	- Retry the internal operation against the Region after it has moved to another server. If the move takes too long (e.g. a configured number of retries has occurred), an appropriate error message should be returned to the client.

###HMaster
+ HMaster-To-ZooKeeper: This is automatically handled as long as there is a backup, so operations should automatically retry themselves up to a configured number of times. If those operations fail after the configured max retry threshold, an appropriate error message should be displayed to the client

###Client
+ Graceful Disconnect: Ensure that there are no hanging resources, and that any open transactions are either committed or rolled back, as appropriate (probably rolled back)
+ Abrupt disconnect: 
	- Roll back any currently open transactions and savepoints.
	- Automatically cancel any currently running query execution


