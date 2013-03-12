package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.derby.impl.sql.execute.index.IndexSet;
import com.splicemachine.derby.impl.sql.execute.index.IndexSetPool;
import com.splicemachine.hbase.BatchProtocol;
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
    public void batchMutate(Collection<Mutation> mutationsToApply) throws IOException {
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)this.getEnvironment();
        indexSet.update(mutationsToApply, rce);

        //apply the local mutations
        HRegion region = rce.getRegion();
        for(Mutation mutation:mutationsToApply){
            mutation.setAttribute(IndexSet.INDEX_UPDATED,IndexSet.INDEX_ALREADY_UPDATED);
            if(mutation instanceof Put)
                region.put((Put)mutation);
            else
                region.delete((Delete)mutation,null,true);
        }
    }

    @Override
    public void deleteFirstAfter(byte[] rowKey,byte[] limit) throws IOException {
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)this.getEnvironment();
        Scan scan = new Scan(rowKey,limit);
        scan.setCaching(1);
        scan.setBatch(1);

        RegionScanner scanner = rce.getRegion().getScanner(scan);
        List<KeyValue> row = Lists.newArrayList();
        if(scanner.next(row)){
            //get the row for the first entry
            byte[] rowBytes =  row.get(0).getRow();
            if(Bytes.compareTo(rowBytes,limit)<0){
                Delete delete = new Delete(rowBytes);
                delete.setAttribute(IndexSet.INDEX_UPDATED,IndexSet.INDEX_ALREADY_UPDATED);
                delete.deleteFamily(HBaseConstants.DEFAULT_FAMILY_BYTES);
                rce.getRegion().delete(delete,null,true);
            }
        }
    }


}
