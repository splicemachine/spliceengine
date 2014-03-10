package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.tools.ResourcePool;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Represents a sequence of values. One per table per instance.
 *
 * This implementation relies on block-allocation and in memory serving to efficiently amortize the cost
 * of getting sequence numbers across many writers. First, this will attempt to reserve a block of numbers
 * from an HBase column. Then each subsequent request for a number will be served from the reserved block, until
 * no more numbers are available in the block, at which point a new block will be requested.
 *
 * @author Scott Fines
 * Created on: 5/28/13
 */
public class Sequence {
    private final AtomicLong remaining = new AtomicLong(0l);
    private final AtomicLong currPosition = new AtomicLong(0l);
    private final HTableInterface sysColumns;
    private final long blockAllocationSize;
    private final byte[] sysColumnsRow;
    private final long startingValue;
    private final long incrementSteps;
    private final Lock updateLock = new ReentrantLock();

    private static final byte[] autoIncrementValueQualifier = Encoding.encode(7);

    public Sequence(HTableInterface sysColumns,
                    long blockAllocationSize,byte[] sysColumnsRow,
                    long startingValue,
                    long incrementSteps) {
        this.sysColumns = sysColumns;
        this.blockAllocationSize = blockAllocationSize;
        this.sysColumnsRow = sysColumnsRow;
        this.startingValue = startingValue;
        this.incrementSteps = incrementSteps;
    }

    public long getNext() throws StandardException {
        // safely get a number to determine whether or not we need to
        // allocate more entries in HBase.
        long allocatedRemaining = remaining.getAndDecrement();
        if(allocatedRemaining<=0){
            allocatedRemaining = allocateBlock();
        }

        return currPosition.getAndAdd(incrementSteps);
    }

    private long allocateBlock() throws StandardException {
        boolean success = false;
        while(!success){
            updateLock.lock();
            try{
            /*
             * Check and make sure that someone else didn't update us in a way that
             * we can use
             */
                long allocated = remaining.getAndDecrement();
                if(allocated>0) return allocated;

                /*
                 * We don't want to use HTable.incrementColumnValue(), because it stores
                 * its data using longs and traditional serialization; this means we would
                 * break every time we tried to read the autoIncrementValue field in SYSCOLUMNS
                 * through SQL.
                 *
                 * Instead, we use check-and-put to do compare-and-swap with longs serialized the same
                 * way other writes and reads expect.
                 */
                //read the current value of the counter from HBase;
                Get currValue = new Get(sysColumnsRow);
                currValue.addColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,autoIncrementValueQualifier);

                Result result = sysColumns.get(currValue);
                long current;
                byte[] currentBytes;
                if(result==null||result.isEmpty()){
                    current=startingValue;
                    currentBytes = null;
                }else{
                    currentBytes = result.getValue(SpliceConstants.DEFAULT_FAMILY_BYTES,autoIncrementValueQualifier);
                    current = Encoding.decodeLong(currentBytes);
                }

                //set the current position of the counter
                currPosition.set(current);

                //attempt to increment it atomically
                long next = current+blockAllocationSize;
                Put put = new Put(sysColumnsRow);
                put.add(SpliceConstants.DEFAULT_FAMILY_BYTES,autoIncrementValueQualifier,Encoding.encode(next));
                success = sysColumns.checkAndPut(sysColumnsRow,
                        SpliceConstants.DEFAULT_FAMILY_BYTES,
                        autoIncrementValueQualifier,currentBytes,put);

                if(success){
                    /*
                     * We have successfully allocated a block of numbers, so
                     * other threads can immediately start using them.
                     *
                     * Since in high-contention situations, we may have many threads waiting for
                     * the next number, we want to make sure that we have allocated this thread
                     * a position as well. Otherwise, we could have a starvation situation
                     * where a single thread is constantly updating the counter through HBase, then
                     * having all of its allocations stolen and never getting one of its own.
                     */
                    remaining.set(blockAllocationSize-1);
                    return blockAllocationSize;
                }
            } catch (IOException e) {
                throw Exceptions.parseException(e);
            } finally{
                updateLock.unlock();
            }
        }
        return 0l; //can never happen
    }

    public void close() throws IOException {
       sysColumns.close();
    }

    /**
     * Used as a unique identifier of Sequence elements (for use in Resource Pools).
     */
    public static class Key implements ResourcePool.Key{
        private final byte[] sysColumnsRow;
        private final HTableInterface table;
        private final long seqConglomId;
        private final int columnNum;
        public final long blockAllocationSize;
        private long autoIncStart;
        private long autoIncrement;

        private boolean systemTableSearched = false;
        private final DataDictionary metaDictionary;

        public Key(HTableInterface table,
                   byte[] sysColumnsRow,
                   long seqConglomId,
                   int columnNum,
                   DataDictionary metaDictionary,
                   long blockAllocationSize) {
            this.sysColumnsRow = sysColumnsRow;
            this.table = table;
            this.seqConglomId = seqConglomId;
            this.columnNum = columnNum;
            this.metaDictionary = metaDictionary;
            this.blockAllocationSize = blockAllocationSize;
        }

        public byte[] getSysColumnsRow(){
            return sysColumnsRow;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof Key)) return false;
            Key key = (Key) o;
            return Arrays.equals(sysColumnsRow, key.sysColumnsRow) && blockAllocationSize == key.blockAllocationSize;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(sysColumnsRow);
        }

        public HTableInterface getTable() {
            return table;
        }

        public long getStartingValue() throws StandardException{
            getStartAndIncrementFromSystemTables();
            if(autoIncStart<=0l) return 0l;
            return autoIncStart;
        }

        public long getIncrementSize() throws StandardException{
            getStartAndIncrementFromSystemTables();
            if(autoIncrement<=0l) return 1l;
            return autoIncrement;
        }

        private void getStartAndIncrementFromSystemTables() throws StandardException {
            if(systemTableSearched) return;
            ConglomerateDescriptor conglomerateDescriptor = metaDictionary.getConglomerateDescriptor(seqConglomId);
            TableDescriptor tableDescriptor = metaDictionary.getTableDescriptor(conglomerateDescriptor.getTableID());
            ColumnDescriptorList columnDescriptorList = tableDescriptor.getColumnDescriptorList();
            for(Object o:columnDescriptorList){
                ColumnDescriptor cd = (ColumnDescriptor)o;
                if(cd.getPosition()==columnNum){
                    autoIncStart = cd.getAutoincStart();
                    autoIncrement = cd.getAutoincInc();
                    break;
                }
            }
            systemTableSearched = true;
        }
    }
}
