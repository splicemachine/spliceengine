package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.uuid.Snowflake;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 7/3/13
 */
public class SnowflakeLoader extends SIConstants {
	DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
    private Snowflake snowflake;

    public synchronized Snowflake load(Integer port) throws IOException {
        if(snowflake!=null)
            return snowflake;

        //get this machine's IP address
        byte[] localAddress = Bytes.add(InetAddress.getLocalHost().getAddress(),Bytes.toBytes(port));
        HTableInterface sequenceTable = SpliceAccessManager.getHTable(SpliceConstants.SEQUENCE_TABLE_NAME_BYTES);
        byte[] counterNameRow = MACHINE_ID_COUNTER.getBytes();
        try{
            Scan scan = new Scan();
            scan.setBatch(100);
            scan.setStartRow(counterNameRow);
            scan.setStopRow(counterNameRow);
            scan.setFilter(derbyFactory.getAllocatedFilter(localAddress));
            ResultScanner scanner = sequenceTable.getScanner(scan);
            try{
                Result result;
                short machineId;
                List<byte[]> availableIds = Lists.newArrayList();
                while((result = scanner.next())!=null){
                    //we found an entry!
                    KeyValue[] raw = result.raw();
                    for(KeyValue kv:raw){
                        if(Bytes.equals(localAddress, kv.getValue())){
                            //this is ours already! we're done!
                            machineId = Encoding.decodeShort(kv.getQualifier());
                            snowflake = new Snowflake(machineId);
                            return snowflake;
                        }else{
                            availableIds.add(kv.getQualifier());
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
        Snowflake snowflake = loader.load(HBaseConfiguration.create().getInt(HConstants.REGIONSERVER_PORT, 6020));
        System.out.println(snowflake.nextUUID());
    }
}
