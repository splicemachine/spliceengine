package com.splicemachine.derby.hbase;

import com.splicemachine.derby.impl.sql.execute.index.IndexSet;
import com.splicemachine.derby.impl.sql.execute.index.IndexSetPool;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Region Observer for managing indices.
 *
 * @author Scott Fines
 * Created on: 2/28/13
 */
public class SpliceIndexObserver extends BaseRegionObserver {
    private static final Logger LOG = Logger.getLogger(SpliceIndexObserver.class);

    private volatile IndexSet indexSet;

    @Override
    public void postOpen(ObserverContext<RegionCoprocessorEnvironment> e) {
        //get the Conglomerate Id. If it's not a table that we can index (e.g. META, ROOT, SYS_TEMP,__TXN_LOG, etc)
        //then don't bother with setting things up
        String tableName = e.getEnvironment().getRegion().getTableDesc().getNameAsString();
        final long conglomId;
        try{
            conglomId = Long.parseLong(tableName);
        }catch(NumberFormatException nfe){
            SpliceLogUtils.debug(LOG, "Unable to parse Conglomerate Id for table %s, indexing is will not be set up", tableName);
            indexSet = IndexSet.noIndex();
            return;
        }

        indexSet = IndexSetPool.getIndex(conglomId);
        SpliceDriver.Service service = new SpliceDriver.Service() {
            @Override
            public boolean start() {
                //get the index set now that we know we can
                indexSet.prepare();
                //now that we know we can start, we don't care what else happens in the lifecycle
                SpliceDriver.driver().deregisterService(this);
                return true;
            }

            @Override
            public boolean shutdown() {
                //we don't care
                return true;
            }
        };

        //register for notifications--allow the registration to tell us if we can go ahead or not
        SpliceDriver.driver().registerService(service);

        super.postOpen(e);
    }

    @Override
    public void postClose(ObserverContext<RegionCoprocessorEnvironment> e, boolean abortRequested) {
        IndexSetPool.releaseIndex(indexSet);
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL) throws IOException {
        indexSet.update(put, e.getEnvironment());
        super.prePut(e, put, edit, writeToWAL);
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e,
                          Delete delete, WALEdit edit, boolean writeToWAL) throws IOException {
        indexSet.update(delete, e.getEnvironment());
        super.preDelete(e, delete, edit, writeToWAL);
    }

/*******************************************************************************************************************/
    /*private helper methods*/

}
