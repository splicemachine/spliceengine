package com.splicemachine.si.coprocessors;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.*;
import com.splicemachine.si.impl.*;
import com.splicemachine.storage.EntryPredicateFilter;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import static com.splicemachine.constants.SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME;

/**
 * An HBase coprocessor that applies SI logic to HBase read/write operations.
 */
public class SIObserver extends SIBaseObserver {
		private static Logger LOG = Logger.getLogger(SIObserver.class);

		@Override
		 public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability writeToWAL) throws IOException {				
			/*
				 * This is relatively expensive--it's better to use the write pipeline when you need to load a lot of rows.
				 */
				if(!tableEnvMatch || put.getAttribute(SIConstants.SI_NEEDED)==null) {
						super.prePut(e,put,edit,writeToWAL);
						return;
				}
				TxnView txn = txnOperationFactory.fromWrites(put);
				boolean isDelete = put.getAttribute(SIConstants.SI_DELETE_PUT)!=null;
				byte[] row = put.getRow();
				boolean isSIDataOnly = true;
				//convert the put into a collection of KVPairs
				Map<byte[],Map<byte[],KVPair>> familyMap = Maps.newHashMap();
				Iterable<KeyValue> keyValues = Iterables.concat(put.getFamilyMap().values());
				for(KeyValue kv:keyValues){
						byte[] family = kv.getFamily();
						byte[] column = kv.getQualifier();
						if(!Bytes.equals(column, SIConstants.PACKED_COLUMN_BYTES)) continue; //skip SI columns

						isSIDataOnly = false;
						byte[] value = kv.getValue();
						Map<byte[],KVPair> columnMap = familyMap.get(family);
						if(columnMap==null){
								columnMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
								familyMap.put(family,columnMap);
						}
						columnMap.put(column,new KVPair(row,value,isDelete? KVPair.Type.DELETE: KVPair.Type.INSERT));
				}
				if(isSIDataOnly){
						byte[] family = SpliceConstants.DEFAULT_FAMILY_BYTES;
						byte[] column = SpliceConstants.PACKED_COLUMN_BYTES;
						byte[] value = HConstants.EMPTY_BYTE_ARRAY;
						Map<byte[],KVPair> columnMap = familyMap.get(family);
						if(columnMap==null){
								columnMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
								familyMap.put(family,columnMap);
						}
						columnMap.put(column,new KVPair(row,value,isDelete? KVPair.Type.DELETE: KVPair.Type.EMPTY_COLUMN));
				}
				boolean processed = false;
				for(Map.Entry<byte[],Map<byte[],KVPair>> family:familyMap.entrySet()){
						byte[] fam = family.getKey();
						Map<byte[],KVPair> cols = family.getValue();
						for(Map.Entry<byte[],KVPair> column:cols.entrySet()){
								OperationStatus[] status = region.bulkWrite(txn,fam,column.getKey(),ConstraintChecker.NO_CONSTRAINT, Collections.singleton(column.getValue()));
								switch(status[0].getOperationStatusCode()){
										case NOT_RUN:
												break;
										case BAD_FAMILY:
												throw new NoSuchColumnFamilyException(status[0].getExceptionMsg());
										case SANITY_CHECK_FAILURE:
												throw new IOException("Sanity Check failure:" + status[0].getExceptionMsg());
										case FAILURE:
												throw new IOException(status[0].getExceptionMsg());
										default:
												processed=true;
								}
						}
				}

//						final boolean processed = transactor.processPut(new HbRegion(e.getEnvironment().getRegion()), rollForward, put);
				if (processed) {
						e.bypass();
						e.complete();
				}
		}


	    @Override
	    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit,
	                          Durability writeToWAL) throws IOException {
        if (tableEnvMatch) {
            if (delete.getAttribute(SUPPRESS_INDEXING_ATTRIBUTE_NAME) == null) {
                throw new RuntimeException("Direct deletes are not supported under snapshot isolation. Instead a Put is expected that will set a record level tombstone.");
            }
        }
        super.preDelete(e, delete, edit, writeToWAL);
    }

    @Override
    public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store,
                                      InternalScanner scanner, ScanType scanType, CompactionRequest compactionRequest) throws IOException {
        if (tableEnvMatch) {
            return region.compactionScanner(scanner);
        } else {
            return super.preCompact(e, store, scanner, scanType, compactionRequest);
        }
    }

    @Override
    protected Filter makeSIFilter(TxnView txn, Filter currentFilter, EntryPredicateFilter predicateFilter, boolean countStar) throws IOException {
				TxnFilter txnFilter = region.packedFilter(txn, predicateFilter, countStar);
				BaseSIFilterPacked siFilter = new BaseSIFilterPacked(txnFilter);
        if (needsCompositeFilter(currentFilter)) {
            return composeFilters(orderFilters(currentFilter, siFilter));
        } else {
            return siFilter;
        }
    }

    
}
