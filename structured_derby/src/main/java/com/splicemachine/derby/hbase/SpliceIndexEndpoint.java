package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.derby.impl.sql.execute.index.IndexSet;
import com.splicemachine.derby.impl.sql.execute.index.IndexSetPool;
import com.splicemachine.derby.utils.Mutations;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.BatchProtocol;
import com.splicemachine.hbase.MutationRequest;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Endpoint to allow special batch operations that the HBase API doesn't explicitly enable
 * by default (such as bulk-processed mutations)
 *
 * @author Scott Fines
 * Created on: 3/11/13
 */
public class SpliceIndexEndpoint extends BaseEndpointCoprocessor implements BatchProtocol{
    private static final Logger LOG = Logger.getLogger(SpliceIndexEndpoint.class);
    private volatile IndexSet indexSet;

    @Override
    public void start(CoprocessorEnvironment env) {
        String tableName = ((RegionCoprocessorEnvironment)env).getRegion().getTableDesc().getNameAsString();
        final long conglomId;
        try{
            conglomId = Long.parseLong(tableName);
        }catch(NumberFormatException nfe){
            SpliceLogUtils.debug(LOG, "Unable to parse conglomerate id for table %s, " +
                    "index management for batch operations will be diabled",tableName);
            indexSet = IndexSet.noIndex();
            super.start(env);
            return;
        }

        indexSet = IndexSetPool.getIndex(conglomId);
        SpliceDriver.Service service = new SpliceDriver.Service(){

            @Override
            public boolean start() {
                indexSet.prepare();
                SpliceDriver.driver().deregisterService(this);
                return true;
            }

            @Override
            public boolean shutdown() {
                return true;
            }
        };
        SpliceDriver.driver().registerService(service);
        super.start(env);
    }

    @Override
    public void batchMutate(MutationRequest mutationsToApply) throws IOException {
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)this.getEnvironment();
        indexSet.update(mutationsToApply.getMutations(), rce);

        //apply the local mutations
        HRegion region = rce.getRegion();
        for(Mutation mutation:mutationsToApply.getMutations()){
            mutation.setAttribute(IndexSet.INDEX_UPDATED,IndexSet.INDEX_ALREADY_UPDATED);
            if(mutation instanceof Put) {
                Pair<Put, Integer> putsAndLocks[] = new Pair[] {new Pair<Put, Integer>((Put) mutation, null)};
                region.put(putsAndLocks);
            } else {
                region.delete((Delete)mutation,null,true);
            }
        }
    }

    @Override
    public void deleteFirstAfter(String transactionId, byte[] rowKey, byte[] limit) throws IOException {
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)this.getEnvironment();
        final HRegion region = rce.getRegion();
        Scan scan = SpliceUtils.createScan(transactionId);
        scan.setStartRow(rowKey);
        scan.setStopRow(limit);
        scan.setCaching(1);
        scan.setBatch(1);

        RegionScanner scanner = region.getScanner(scan);
        List<KeyValue> row = Lists.newArrayList();
        if(scanner.next(row)){
            //get the row for the first entry
            byte[] rowBytes =  row.get(0).getRow();
            if(Bytes.compareTo(rowBytes,limit)<0){
                Mutation mutation = Mutations.getDeleteOp(transactionId,rowBytes);
                mutation.setAttribute(IndexSet.INDEX_UPDATED,IndexSet.INDEX_ALREADY_UPDATED);
                if(mutation instanceof Put)
                    region.put(new Pair[]{Pair.newPair((Put)mutation,null)});
                else
                    region.delete((Delete)mutation,null,true);
            }
        }
    }

}
