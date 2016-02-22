package com.splicemachine.uuid;

import com.google.common.collect.Lists;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.derby.impl.sql.execute.operations.OperationConfiguration;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 7/3/13
 */
public class SnowflakeLoader{
    public static final long MAX_MACHINE_ID = 0xffff; //12 bits of 1s is the maximum machine id available

    private final PartitionFactory tableFactory;
    private final TxnOperationFactory opFactory;
    private final DataFilterFactory filterFactory;

    private Snowflake snowflake;
    private volatile short usedMachineId;

    public SnowflakeLoader(PartitionFactory tableFactory,TxnOperationFactory opFactory,DataFilterFactory filterFactory){
        this.tableFactory=tableFactory;
        this.opFactory=opFactory;
        this.filterFactory=filterFactory;
    }

    public synchronized Snowflake load(Integer port) throws IOException{
        if(snowflake!=null)
            return snowflake;

        //get this machine's IP address
        byte[] localAddress=Bytes.concat(Arrays.asList(InetAddress.getLocalHost().getAddress(),Bytes.toBytes(port)));
        byte[] counterNameRow=Bytes.toBytes(SIConstants.MACHINE_ID_COUNTER);
        try(Partition sequenceTable = tableFactory.getTable(OperationConfiguration.SEQUENCE_TABLE_NAME)){
            DataScan scan = opFactory.newDataScan(null)
                    .batchCells(100)
                    .startKey(counterNameRow)
                    .stopKey(counterNameRow)
                    .filter(filterFactory.allocatedFilter(localAddress));

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
                            usedMachineId = machineId;
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
                        DataPut put=opFactory.newDataPut(null,counterNameRow);
                        put.addCell(SIConstants.DEFAULT_FAMILY_BYTES,next,localAddress);
                        boolean success=sequenceTable.checkAndPut(counterNameRow,SIConstants.DEFAULT_FAMILY_BYTES,next,SIConstants.EMPTY_BYTE_ARRAY,put);
                        if(success){
                            machineId=Encoding.decodeShort(next);
                            usedMachineId = machineId;
                            snowflake=new Snowflake(machineId);
                            return snowflake;
                        }
                    }
                }
                //someone got to all the already allocated ones, so get a new counter value, and insert it
                long next = sequenceTable.increment(counterNameRow,SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.COUNTER_COL,1l);
                if(next>MAX_MACHINE_ID){
                    throw new IOException("Unable to allocate a machine id--too many taken already");
                }else{
                    machineId=(short)next;
                    //list our position for everyone else
                    DataPut put=opFactory.newDataPut(null,counterNameRow);
                    put.addCell(SIConstants.DEFAULT_FAMILY_BYTES,Encoding.encode(machineId),localAddress);
                    sequenceTable.put(put);

                    snowflake=new Snowflake(machineId);
                    usedMachineId = machineId;
                    return snowflake;
                }
            }
        }
    }

    public synchronized void unload() throws Exception{
        byte[] counterNameRow=Bytes.toBytes(SIConstants.MACHINE_ID_COUNTER);
        SIDriver driver=SIDriver.driver();
        try(Partition table = driver.getTableFactory().getTable(OperationConfiguration.SEQUENCE_TABLE_NAME)){
            DataPut put=driver.getOperationFactory().newDataPut(null,counterNameRow);
            put.addCell(SIConstants.DEFAULT_FAMILY_BYTES,Encoding.encode(usedMachineId),SIConstants.EMPTY_BYTE_ARRAY);

            table.put(put);
        }
        snowflake = null;
        usedMachineId = -1;
    }
}
