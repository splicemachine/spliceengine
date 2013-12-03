package com.splicemachine.si.txn;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.TimestampSource;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

/**
 * Higher-performance ZooKeeper-based Timestamp Source.
 *
 * The typical way to implement a ZooKeeper-based Sequential ID generator
 * is to use ephemeral_sequential znodes, and then take the sequence id
 * off the end of newly created nodes. This approach works, but it suffers
 * from a number of limitations:
 *
 * <ol>
 *     <li>Too Few Timestamps: ZooKeeper can only create up to Integer.MAX_VALUE sequential znodes
 *     under a single parent node</li>
 *     <li>Destabilizing: ZooKeeper keeps each znode in memory, resulting in a memory leak unless you delete
 *     znodes immediately after creating them. Benchmarking this approach indicates that even asynchronously deleting
 *     nodes imposes a significant performance penalty</li>
 *     <li>Slow: Creating a child znode is a relatively expensive operation.</li>
 * </ol>
 *
 * This implementation seeks to avoid these problems, by taking advantage of ZooKeeper's internal
 * MVCC system. In particular, it makes use of the internal version information on a single node.
 *
 * Whenever you update a single node in ZooKeeper, it updates an internal version number, and returns
 * that to you. This version number is monotonic and increasing (by definition), so it's our effective
 * timestamp. Because modifying a single znode is very cheap, this is quite fast, and because it only ever
 * modifies a single znode, it requires very little memory from ZooKeeper (and thus is much more stable).
 *
 * However, version numbers are integers, so we still have a problem with too-few transaction ids. We
 * want a long, but we only have an int. To avoid this, we use a fixed znode called the "high-bits node". This
 * znode's version is the highest 32 bits of the transaction timestamp, and the data is a pointer to a second
 * "counter" znode, whose version is the lowest 32 bits of the timestamp.
 *
 * When the counter znode overflows, we re-read the high-bits znode. If it has already changed (e.g. a new
 * version number and a new counter znode pointer), then we re-read the new counter znode to get the new 32-bits. If
 * the EMPTY_DATA has not changed, then we create a new counter znode, then attempt to set it to the high-bits znode in
 * a CompareAndSet operation. This CompareAndSet operation can lose out, in which case we re-read the high-bits  and
 * reset to the new counter node.
 *
 * @author Scott Fines
 * Created on: 11/6/13
 */
public class ZooKeeperStatTimestampSource implements TimestampSource {
    private static final Logger LOG = Logger.getLogger(ZooKeeperStatTimestampSource.class);
    private static final byte[] EMPTY_DATA = new byte[]{0x01};

    private final String highBitsTransactionPath;
    private RecoverableZooKeeper rzk;

    private volatile String counterTransactionPath;
    private volatile long highBits; //the high 31-bits of the transaction id

    private String minimumTransactionPath;

    public ZooKeeperStatTimestampSource(RecoverableZooKeeper rzk, String transactionPath) {
        this.rzk = rzk;
        this.highBitsTransactionPath = transactionPath;

        initialize();
    }

    @Override
    public long nextTimestamp() {
        try {
            Stat stat = rzk.setData(counterTransactionPath, EMPTY_DATA, -1);
            int version = stat.getVersion();
            if(version<0){
                //we've got an overflow on this counter node--reset the counter and then retry
                resetCounterNode();
                return nextTimestamp();
            }else
                return version | highBits; //set the high 31 bits
        } catch (KeeperException e) {
            if(e.code()== KeeperException.Code.NONODE){
                /*
                 * We deleted this counter node, which means that it overflowed
                 * on someone else's watch and they cleaned up before we got a chance
                 * to take part. Reset the counter node and retry
                 */
                resetCounterNode();
                return nextTimestamp();
            }
            throw new RuntimeException("Unable to create a new timestamp",e);
        } catch (InterruptedException e) {
            throw new RuntimeException("Unable to create a new timestamp",e);
        }
    }

    @Override
    public void rememberTimestamp(long timestamp) {
        byte[] data = Bytes.toBytes(timestamp);
        try {
            rzk.setData(SpliceConstants.zkSpliceMinimumActivePath, data, -1);
        } catch (Exception e) {
            LOG.error("Couldn't remember timestamp", e);
            throw new RuntimeException("Couldn't remember timestamp",e);
        }
    }

    @Override
    public long retrieveTimestamp() {
        byte[] data;
        try {
            data = rzk.getData(SpliceConstants.zkSpliceMinimumActivePath, false, null);
        } catch (Exception e) {
            LOG.error("Couldn't retrieve minimum timestamp", e);
            return 0;
        }
        return Bytes.toLong(data);
    }

/******************************************************************************/
    /*private helper methods*/

    private void resetCounterNode(){
        /*
         * First re-read the high bits. If the new high-bits is bigger than
         * our current one, then point the counter to the new znode
         */
        Stat highBitStat = new Stat();
        try{
            byte[] data = rzk.getData(highBitsTransactionPath,false,highBitStat);

            long newHighBits = (long)(highBitStat.getVersion()-1)<<32;
            if(newHighBits<highBits){
                /*
                 * This can only happen if the newHighBits has overflowed--e.g.
                 * that we have run out of timestamps. There's nothing
                 * we can do, we're screwed.
                 */
                throw new IllegalStateException("Exceeded the maximum number of transactions "+
                        "available in Splice. How did you even do this, " +
                        "we have 2^63 possible transaction ids! You're screwed. Call SpliceMachine Support" +
                        "and ask for another database");
            }else if(newHighBits>highBits){
                /*
                 * Someone else modified the highBit field already, so just update out local
                 * pointer and go from there
                 */
                highBits = newHighBits;
                counterTransactionPath = highBitsTransactionPath+"/"+Bytes.toString(data);
            }else{
                /*
                 * The high bits field is the same. No one has detected the overflow yet (that we
                 * know of). Update them yourself. If we are unable to update them ourselves,
                 * then retry resetting the counter node, because someone else did.
                 */
                if(!incrementHighBits(highBitStat)){
                    resetCounterNode();
                }
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Unable to get timestamp",e);
        } catch (KeeperException e) {
            throw new RuntimeException("Unable to get timestamp",e);
        }
    }

    private boolean incrementHighBits(Stat existingHighBits) {
        String newCounterPath = null;
        try{
            newCounterPath = rzk.create(highBitsTransactionPath + "/counter-", EMPTY_DATA, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);

            Stat newStat = rzk.setData(highBitsTransactionPath, Bytes.toBytes(newCounterPath), existingHighBits.getVersion());
            /*
             * We successfully set the high bits. Asynchronously delete the old counter field, then
             * return true.
             */
            String oldCounter = counterTransactionPath;
            counterTransactionPath = newCounterPath;
            asyncDelete(oldCounter);
            setHighBits(newStat.getVersion());
            return true;
        } catch (InterruptedException e) {
            throw new RuntimeException("Unable to update high bits in timestamp",e);
        } catch (KeeperException e) {
            if(e.code()== KeeperException.Code.BADVERSION){
                /*
                 * We lost the CAS operation. Asynchronously delete the counter path we
                 * created to avoid a memory leak, then return.
                 */
                asyncDelete(newCounterPath);
                return false;
            }
            throw new RuntimeException("Unable to update high bits in timestamp",e);
        }
    }

    private void asyncDelete(final String newCounterPath) {
        rzk.getZooKeeper().delete(newCounterPath,-1,new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                if(LOG.isDebugEnabled()){
                    LOG.debug("Deleted "+ newCounterPath);
                }
            }
        },this);
    }

    private void initialize() {
        Stat highBitStat = new Stat();
        try {
            byte[] data = rzk.getData(highBitsTransactionPath, false, highBitStat);
            counterTransactionPath = Bytes.toString(data);
            setHighBits(highBitStat.getVersion());
        } catch (KeeperException e) {
            /*
             * Note: We assume that the highBitsTransactionPath has been safely created
             * externally to us.
             */
            throw new RuntimeException("Unable to initialize the TimestampSource", e);
        } catch (InterruptedException e) {
            throw new RuntimeException("Unable to initialize the TimestampSource", e);
        }
    }


    private void setHighBits(int bits) {
        /*
         * we subtract off 1 because the high bits node will have an initial version
         * of 1, not 0, and why lose out on 2^32 timestamps?
         */
        highBits = (long)(bits-1) <<32;
        if(highBits<0){
            /*
             * if our high bits is negative, then we have exceeded the maximum
             * number of positive longs available in the system.
             * There's nothing we can do, Splice is done for.
             */
            throw new IllegalStateException("Exceeded the maximum number of transactions "+
                    "available in Splice. How did you even do this, " +
                    "we have 2^63 possible transaction ids! You're screwed. Call SpliceMachine Support" +
                    "and ask for another database");
        }
    }

    public static void main(String...args) throws Exception{
        System.out.println();
        RecoverableZooKeeper rzk = new RecoverableZooKeeper("localhost:2181",10000,new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println(event);
            }
        },3,1000);

        ZooKeeperStatTimestampSource zooKeeperTimestampSource = new ZooKeeperStatTimestampSource(rzk, "/txn");
        for(int i=0;i<100;i++){
            System.out.println(zooKeeperTimestampSource.nextTimestamp());
        }
    }
}
