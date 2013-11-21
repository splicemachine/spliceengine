package com.splicemachine.derby.impl.sql.execute.operations;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.storage.AndPredicate;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.OrPredicate;
import com.splicemachine.storage.Predicate;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 10/1/13
 */
public class MultiProbeDerbyScanInformation extends DerbyScanInformation{
    private DataValueDescriptor[] probeValues;
    public MultiProbeDerbyScanInformation(String resultRowAllocatorMethodName,
                                          String startKeyGetterMethodName,
                                          String stopKeyGetterMethodName,
                                          String scanQualifiersField,
                                          long conglomId,
                                          int colRefItem,
                                          boolean sameStartStopPosition,
                                          int startSearchOperator,
                                          int stopSearchOperator,
                                          DataValueDescriptor[] probeValues) {
        super(resultRowAllocatorMethodName, startKeyGetterMethodName, stopKeyGetterMethodName,
                scanQualifiersField, conglomId, colRefItem, sameStartStopPosition, startSearchOperator, stopSearchOperator);
        this.probeValues = probeValues;
    }

    @Deprecated
    public MultiProbeDerbyScanInformation() { }

    @Override
    public Scan getScan(String txnId) throws StandardException {
        /*
         * We must build the proper scan here in pieces
         */
        BitSet colsToReturn = new BitSet();
        FormatableBitSet accessedCols = getAccessedColumns();
        if(accessedCols!=null){
            for(int i=accessedCols.anySetBit();i>=0;i=accessedCols.anySetBit(i)){
                colsToReturn.set(i);
            }
        }
        MultiRangeFilter.Builder builder= new MultiRangeFilter.Builder();
        ObjectArrayList<Predicate> allScanPredicates = ObjectArrayList.newInstanceWithCapacity(probeValues.length);
        SpliceConglomerate conglomerate = getConglomerate();
        for(DataValueDescriptor probeValue:probeValues){
            try{
                ExecIndexRow startPosition = getStartPosition();
                ExecIndexRow stopPosition = sameStartStopPosition? startPosition:getStopPosition();
                if(sameStartStopPosition)
                    startSearchOperator = ScanController.NA;

                if(startPosition!=null)
                    startPosition.getRowArray()[0] = probeValue; //TODO -sf- is this needed?
                if(sameStartStopPosition||stopPosition.nColumns()>1){
                    stopPosition.getRowArray()[0] = probeValue;
                }
                Qualifier[][] scanQualifiers = populateQualifiers();
                ObjectArrayList<Predicate> scanPredicates;
                if(scanQualifiers!=null){
                    scanPredicates = Scans.getQualifierPredicates(scanQualifiers);
                    if(accessedCols!=null){
                        for(Qualifier[] qualifierList:scanQualifiers){
                            for(Qualifier qualifier:qualifierList){
                                colsToReturn.set(qualifier.getColumnId());
                            }
                        }
                    }
                }else{
                    scanPredicates = ObjectArrayList.newInstanceWithCapacity(0);
                }

                //get the start and stop keys for the scan
                Pair<byte[],byte[]> startAndStopKeys =
                        Scans.getStartAndStopKeys(startPosition.getRowArray(),startSearchOperator,stopPosition.getRowArray(),stopSearchOperator,conglomerate.getAscDescInfo());
                builder.addRange(startAndStopKeys.getFirst(),startAndStopKeys.getSecond());
                if(startPosition!=null && startSearchOperator != ScanController.GT){
                    Predicate indexPredicate = Scans.generateIndexPredicate(startPosition.getRowArray(),startSearchOperator);
                    if(indexPredicate!=null)
                        scanPredicates.add(indexPredicate);
                }
                allScanPredicates.add(new AndPredicate(scanPredicates));
            }catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        Predicate finalPredicate  = new OrPredicate(allScanPredicates);
        Scan scan = SpliceUtils.createScan(txnId);
        scan.addColumn(SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY);
        EntryPredicateFilter epf = new EntryPredicateFilter(colsToReturn, ObjectArrayList.from(finalPredicate));
        scan.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,epf.toBytes());
        MultiRangeFilter filter = builder.build();
        scan.setStartRow(filter.getMinimumStart());
        scan.setStopRow(filter.getMaximumStop());
        scan.setFilter(filter);

        return scan;
    }

}
