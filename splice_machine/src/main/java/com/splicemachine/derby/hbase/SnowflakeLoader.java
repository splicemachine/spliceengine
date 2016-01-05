package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;
import com.splicemachine.uuid.Snowflake;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 7/3/13
 */
public class SnowflakeLoader{
    public static final String SEQUENCE_TABLE_NAME = "SPLICE_SEQUENCES";
    public static final String MACHINE_ID_COUNTER = "MACHINE_IDS";
    public static final byte[] COUNTER_COL = Bytes.toBytes("c");
    public static final long MAX_MACHINE_ID = 0xffff; //12 bits of 1s is the maximum machine id available

    private Snowflake snowflake;

    public synchronized Snowflake load(Integer port) throws IOException{
        if(snowflake!=null)
            return snowflake;
        SIDriver driver = SIDriver.driver();

        //get this machine's IP address
        byte[] localAddress=Bytes.concat(Arrays.asList(InetAddress.getLocalHost().getAddress(),Bytes.toBytes(port)));
        byte[] counterNameRow=MACHINE_ID_COUNTER.getBytes();
        try(Partition sequenceTable = driver.getTableFactory().getTable(SEQUENCE_TABLE_NAME)){
            DataScan scan = driver.getOperationFactory().newDataScan(null)
                    .batchCells(100)
                    .startKey(counterNameRow)
                    .stopKey(counterNameRow)
                    .filter(driver.filterFactory().allocatedFilter(localAddress));

            try(DataResultScanner scanner = sequenceTable.openResultScanner(scan)){
                DataResult result;
                short machineId;
                List<byte[]> availableIds=Lists.newArrayList();
                while((result=scanner.next())!=null){
                    //we found an entry!
                    for(DataCell kv : result){
                        if(Bytes.equals(localAddress,kv.value())){
                            //this is ours already! we're done!
                            machineId=Encoding.decodeShort(kv.qualifier());
                            snowflake=new Snowflake(machineId);
                            return snowflake;
                        }else{
                            availableIds.add(kv.qualifier());
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
                    for(byte[] next : availableIds){
                        DataPut put=driver.getOperationFactory().newDataPut(null,counterNameRow);
                        put.addCell(SIConstants.DEFAULT_FAMILY_BYTES,next,localAddress);
                        boolean success=sequenceTable.checkAndPut(counterNameRow,SIConstants.DEFAULT_FAMILY_BYTES,next,SIConstants.EMPTY_BYTE_ARRAY,put);
                        if(success){
                            machineId=Encoding.decodeShort(next);
                            snowflake=new Snowflake(machineId);
                            return snowflake;
                        }
                    }
                }
                //someone got to all the already allocated ones, so get a new counter value, and insert it
                long next = sequenceTable.increment(counterNameRow,SIConstants.DEFAULT_FAMILY_BYTES,COUNTER_COL,1l);
                if(next>MAX_MACHINE_ID){
                    throw new IOException("Unable to allocate a machine id--too many taken already");
                }else{
                    machineId=(short)next;
                    //list our position for everyone else
                    DataPut put=driver.getOperationFactory().newDataPut(null,counterNameRow);
                    put.addCell(SIConstants.DEFAULT_FAMILY_BYTES,Encoding.encode(machineId),localAddress);
                    sequenceTable.put(put);

                    snowflake=new Snowflake(machineId);
                    return snowflake;
                }
            }
        }
    }

    public static void unload(short machineId) throws Exception{
        byte[] counterNameRow=MACHINE_ID_COUNTER.getBytes();
        SIDriver driver=SIDriver.driver();
        try(Partition table = driver.getTableFactory().getTable(SEQUENCE_TABLE_NAME)){
            DataPut put=driver.getOperationFactory().newDataPut(null,counterNameRow);
            put.addCell(SIConstants.DEFAULT_FAMILY_BYTES,Encoding.encode(machineId),SIConstants.EMPTY_BYTE_ARRAY);

            table.put(put);
        }
    }
}
