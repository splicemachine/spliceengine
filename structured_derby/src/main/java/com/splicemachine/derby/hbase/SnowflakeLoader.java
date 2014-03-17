package com.splicemachine.derby.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.utils.Snowflake;

/**
 * @author Scott Fines
 *         Created on: 7/3/13
 */
public class SnowflakeLoader {
    private static final String MACHINE_ID_COUNTER = "MACHINE_IDS";

    private static final byte[] COUNTER_COL = "c".getBytes();
    private static final long MAX_MACHINE_ID = 0xffff; //12 bits of 1s is the maximum machine id available

    private Snowflake snowflake;

    public synchronized Snowflake load() throws IOException {
        if(snowflake!=null)
            return snowflake;

        //get this machine's IP address
        byte[] localAddress = InetAddress.getLocalHost().getAddress();
        HTableInterface sequenceTable = SpliceAccessManager.getHTable(SpliceConstants.SEQUENCE_TABLE_NAME_BYTES);
        byte[] counterNameRow = MACHINE_ID_COUNTER.getBytes();
        try{
            Scan scan = new Scan();
            scan.setBatch(100);
            scan.setStartRow(counterNameRow);
            scan.setStopRow(counterNameRow);
            scan.setFilter(new AllocatedFilter(localAddress));

            ResultScanner scanner = sequenceTable.getScanner(scan);
            try{
                Result result;
                short machineId;
                List<byte[]> availableIds = Lists.newArrayList();
                while((result = scanner.next())!=null){
                    //we found an entry!
                    Cell[] raw = result.rawCells();
                    for(Cell kv:raw){
                        if(Bytes.equals(localAddress, kv.getValueArray())){
                            //this is ours already! we're done!
                            machineId = Encoding.decodeShort(kv.getQualifierArray());
                            snowflake = new Snowflake(machineId);
                            return snowflake;
                        }else{
                            availableIds.add(kv.getQualifierArray());
                        }
                    }
                }
                //we've accumulated a list of availableIds--but that list might be empty
                if(!availableIds.isEmpty()){
                    /*
                     * There's an empty element available, so try and grab it.
                     * Of course, other machines might get there first, so we'll have to loop through
                     * the get attempts
                     */
                    for(byte[] next:availableIds){
                        Put put = new Put(counterNameRow);
                        put.add(SpliceConstants.DEFAULT_FAMILY_BYTES,next,localAddress);
                        boolean success = sequenceTable.checkAndPut(counterNameRow,SpliceConstants.DEFAULT_FAMILY_BYTES,next, HConstants.EMPTY_START_ROW,put);
                        if(success){
                            machineId = Encoding.decodeShort(next);
                            snowflake = new Snowflake(machineId);
                            return snowflake;
                        }
                    }
                }
                //someone got to all the already allocated ones, so get a new counter value, and insert it
                long next = sequenceTable.incrementColumnValue(counterNameRow,SpliceConstants.DEFAULT_FAMILY_BYTES,COUNTER_COL,1l);
                if(next > MAX_MACHINE_ID){
                    throw new IOException("Unable to allocate a machine id--too many taken already");
                }else{
                    machineId = (short)next;
                    //list our position for everyone else
                    Put put = new Put(counterNameRow);
                    put.add(SpliceConstants.DEFAULT_FAMILY_BYTES,Encoding.encode(machineId),localAddress);
                    sequenceTable.put(put);

                    snowflake = new Snowflake(machineId);
                    return snowflake;
                }
            }finally{
                scanner.close();
            }
        }finally{
            sequenceTable.close();
        }
    }

    public static void unload(short machineId) throws Exception{
        byte[] counterNameRow = MACHINE_ID_COUNTER.getBytes();
        HTableInterface table = SpliceAccessManager.getHTable(counterNameRow);
        try{
            Put put = new Put(counterNameRow);
            put.add(SpliceConstants.DEFAULT_FAMILY_BYTES,Encoding.encode(machineId),HConstants.EMPTY_START_ROW);

            table.put(put);
        }finally{
            table.close();
        }
    }

    public static void main(String... args) throws Exception{
        new SpliceAccessManager();// initialize the table pool
        SnowflakeLoader loader = new SnowflakeLoader();
        Snowflake snowflake = loader.load();
        System.out.println(snowflake.nextUUID());
    }

    public static class AllocatedFilter extends FilterBase {
        @SuppressWarnings("unused")
		private static final long serialVersionUID = 2l;

        private byte[] addressMatch;
        private boolean foundMatch;

        public AllocatedFilter() {
            super();
        }

        public AllocatedFilter(byte[] localAddress) {
            this.addressMatch = localAddress;
            this.foundMatch=false;
        }

        // TODO JC - protobuf
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(addressMatch.length);
            out.write(addressMatch);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            addressMatch = new byte[in.readInt()];
            in.readFully(addressMatch);
        }

        @Override
        public ReturnCode filterKeyValue(Cell ignored) {
            if(foundMatch)
                return ReturnCode.NEXT_ROW; //can skip the remainder, because we've already got an entry allocated
            byte[] value = CellUtil.cloneValue(ignored);
            if(Bytes.equals(addressMatch,value)){
                foundMatch= true;
                return ReturnCode.INCLUDE;
            }else if(value.length!=0 || Bytes.equals(value,COUNTER_COL)){
                //a machine has already got this id -- also skip the counter column, since we don't need that
                return ReturnCode.SKIP;
            }
            return ReturnCode.INCLUDE; //this is an available entry
        }
    }
}
