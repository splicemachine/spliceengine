package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.derby.impl.sql.execute.index.IndexManager;
import com.splicemachine.derby.impl.sql.execute.index.IndexUtils;
import com.splicemachine.derby.impl.sql.execute.index.SpliceIndexProtocol;
import com.splicemachine.derby.stats.SinkStats;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 3/11/13
 */
public class SpliceIndexManagementEndpoint extends BaseEndpointCoprocessor implements SpliceIndexProtocol{

    @Override
    public SinkStats buildIndex(long indexConglomId,long baseConglomId,
                                int[] indexColsToBaseColMap,boolean isUnique) throws IOException {
        SinkStats.SinkAccumulator accumulator = SinkStats.uniformAccumulator();
        accumulator.start();

        //get a region scanner
        HRegion region = ((RegionCoprocessorEnvironment) this.getEnvironment()).getRegion();
        Scan regionScan = new Scan();
        regionScan.setCaching(100);
        //we want to scan the entire region in order to update the index correctly
        regionScan.setStartRow(region.getStartKey());
        regionScan.setStopRow(region.getEndKey());

        //only pull the columns that we care about
        for(int mainTablePos:indexColsToBaseColMap){
            regionScan.addColumn(HBaseConstants.DEFAULT_FAMILY_BYTES,Integer.toString(mainTablePos-1).getBytes());
        }

        RegionScanner sourceScanner = region.getScanner(regionScan);

        //get an indexManager to handle our stuff
        IndexManager indexManager = IndexManager.create(indexConglomId, indexColsToBaseColMap,isUnique);

        //get an HTable from the environment
        HTable table = new HTable(Long.toString(indexConglomId).getBytes());
        //disable flush so that things go faster
        table.setAutoFlush(false);

        List<KeyValue> nextRow = Lists.newArrayListWithExpectedSize(indexColsToBaseColMap.length);
        boolean shouldContinue = true;
        try{
            while(shouldContinue){
                nextRow.clear();
                long start = System.nanoTime();
                shouldContinue = sourceScanner.next(nextRow);
                List<Put> indexPuts = indexManager.translateResult(nextRow);
                accumulator.processAccumulator().tick(indexPuts.size(),System.nanoTime()-start);

                start = System.nanoTime();
                table.put(indexPuts);
                accumulator.sinkAccumulator().tick(indexPuts.size(),System.nanoTime()-start);
            }
            table.flushCommits();
            table.close();
        }catch(RetriesExhaustedWithDetailsException rewde){
            //unwrap DoNotRetryIOExceptions
            List<Throwable> errors = rewde.getCauses();
            for(Throwable t:errors){
                if(t instanceof DoNotRetryIOException) throw (DoNotRetryIOException)t;
            }
            //no DoNotTry errors, so throw whatever it was and allow the caller to retry
            throw rewde;
        }

        //add this to the index observer for this part of the table
        IndexUtils.getIndex(baseConglomId).addIndex(indexManager);
        return accumulator.finish();
    }

    @Override
    public void dropIndex(long indexConglomId,long baseConglomId) throws IOException {
        IndexManager dropManager = IndexManager.emptyTable(indexConglomId, new int[]{}, false);
        IndexUtils.getIndex(baseConglomId).dropIndex(dropManager);
    }
}
