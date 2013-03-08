package com.splicemachine.derby.hbase;

import com.splicemachine.derby.impl.sql.execute.index.IndexSet;
import com.splicemachine.derby.impl.sql.execute.index.IndexUtils;
import com.splicemachine.hbase.BatchProtocol;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;

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

        indexSet = IndexUtils.getIndex(conglomId);
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
            if(mutation instanceof Put)
                region.put((Put)mutation);
            else
                region.delete((Delete)mutation,null,true);
        }
    }


}
