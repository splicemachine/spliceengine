
package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.storage.MultiScanExecRowProvider;
import com.splicemachine.derby.impl.storage.MultiScanRowProvider;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.storage.AndPredicate;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.OrPredicate;
import com.splicemachine.storage.Predicate;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
// These are for javadoc "@see" tags.
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

/**
 * Result set that fetches rows from a scan by "probing" the underlying
 * table with a given list of values.  Repeated calls to nextRow()
 * will first return all rows matching probeValues[0], then all rows matching
 * probeValues[1], and so on (duplicate probe values are ignored).  Once all
 * matching rows for all values in probeValues have been returned, the call
 * to nextRow() will return null, thereby ending the scan. The
 * expectation is that this kind of result set only ever appears beneath
 * some other top-level result set (esp. IndexRowToBaseRowResultSet), in
 * which case all result sets higher up in the result set tree will just
 * see a stream of rows satisfying the list of probe values.
 *
 * Currently this type of result is used for evaluation of IN lists, where
 * the user wants to retrieve all rows for which some target column has a
 * value that equals one of values in the IN list.  In that case the IN list
 * values are represented by the probeValues array.
 *
 * Most of the work for this class is inherited from TableScanResultSet. 
 * This class overrides four public methods and two protected methods
 * from TableScanResultSet.  In all cases the methods here set probing
 * state and then call the corresponding methods on "super".
 */
public class MultiProbeTableScanOperation extends TableScanOperation  {
    private static final long serialVersionUID = 1l;
    /** The values with which we will probe the table. */
    protected DataValueDescriptor [] probeValues;
//    /**
//     * The values with which we will probe the table, as they were passed to
//     * the constructor. We need to keep them unchanged in case the result set
//     * is reused when a statement is re-executed (see DERBY-827).
//     */
//    protected DataValueDescriptor [] origProbeValues;

//    /**
//     * 0-based position of the <b>next</b> value to lookup w.r.t. the probe
//     * values list.
//     */
//    protected int probeValIndex;

    /**
     * Indicator as to which type of sort we need: ASCENDING, DESCENDING,
     * or NONE (NONE is represented by "RowOrdering.DONTCARE" and is used
     * for cases where all necessary sorting occurred at compilation time).
     */
    private int sortRequired;

//    /**
//     * Tells whether or not we should skip the next attempt to (re)open the
//     * scan controller. If it is {@code true} it means that the previous call
//     * to {@link #initStartAndStopKey()} did not find a new probe value, which
//     * means that the probe list is exhausted and we shouldn't perform a scan.
//     */
//    private boolean skipNextScan;

    /**
     * Only used for Serialization, DO NOT USE
     */
    @Deprecated
    public MultiProbeTableScanOperation() { }

    /**
     * Constructor.  Just save off the relevant probing state and pass
     * everything else up to TableScanResultSet.
     * 
     * @see ResultSetFactory#getMultiProbeTableScanResultSet
     * @exception StandardException thrown on failure to open
     */
    public MultiProbeTableScanOperation(long conglomId,
        StaticCompiledOpenConglomInfo scoci, Activation activation, 
        GeneratedMethod resultRowAllocator, 
        int resultSetNumber,
        GeneratedMethod startKeyGetter, int startSearchOperator,
        GeneratedMethod stopKeyGetter, int stopSearchOperator,
        boolean sameStartStopPosition,
        String qualifiersField,
        DataValueDescriptor [] probingVals,
        int sortRequired,
        String tableName,
        String userSuppliedOptimizerOverrides,
        String indexName,
        boolean isConstraint,
        boolean forUpdate,
        int colRefItem,
        int indexColItem,
        int lockMode,
        boolean tableLocked,
        int isolationLevel,
        boolean oneRowScan,
        double optimizerEstimatedRowCount,
        double optimizerEstimatedCost)
            throws StandardException
    {
        /* Note: We use '1' as rows per read because we do not currently
         * allow bulk fetching when multi-probing.  If that changes in
         * the future then we will need to update rowsPerRead accordingly.
         */
        super(conglomId,
            scoci,
            activation,
            resultRowAllocator,
            resultSetNumber,
            startKeyGetter,
            startSearchOperator,
            stopKeyGetter,
            stopSearchOperator,
            sameStartStopPosition,
            qualifiersField,
            tableName,
            userSuppliedOptimizerOverrides,
            indexName,
            isConstraint,
            forUpdate,
            colRefItem,
            indexColItem,
            lockMode,
            tableLocked,
            isolationLevel,
            1, // rowsPerRead
            oneRowScan,
            optimizerEstimatedRowCount,
            optimizerEstimatedCost);

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                (probingVals != null) && (probingVals.length > 0),
                "No probe values found for multi-probe scan.");
        }
        this.probeValues = probingVals;
        this.sortRequired = sortRequired;
        if(this.sortRequired!=RowOrdering.DONTCARE)
            sortProbeValues();
        recordConstructorTime(); 
    }

    private void sortProbeValues() {
        if(sortRequired==RowOrdering.ASCENDING)
            Arrays.sort(probeValues);
        else
            Arrays.sort(probeValues,Collections.reverseOrder());
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        super.init(context);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        probeValues = new DataValueDescriptor[in.readInt()];
        ArrayUtil.readArrayItems(in,probeValues);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        ArrayUtil.writeArray(out,probeValues);
    }

    @Override
    protected Scan buildScan() {
        /*
         * We must build the proper scan here in pieces
         */
        BitSet colsToReturn = new BitSet();
        if(accessedCols!=null){
            for(int i=accessedCols.anySetBit();i>=0;i=accessedCols.anySetBit(i)){
                colsToReturn.set(i);
            }
        }
        MultiRangeFilter.Builder builder= new MultiRangeFilter.Builder();
        List<Predicate> allScanPredicates = Lists.newArrayListWithExpectedSize(probeValues.length);
        for(DataValueDescriptor probeValue:probeValues){
            try{
                populateStartAndStopPositions();
                if(startPosition!=null)
                    startPosition.getRowArray()[0] = probeValue; //TODO -sf- is this needed?
                if(sameStartStopPosition||stopPosition.nColumns()>1){
                    stopPosition.getRowArray()[0] = probeValue;
                }
                populateQualifiers();
                List<Predicate> scanPredicates;
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
                    scanPredicates = Lists.newArrayListWithExpectedSize(0);
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
            }catch(StandardException e){
                SpliceLogUtils.logAndThrowRuntime(LOG, e);
            } catch (IOException e) {
                SpliceLogUtils.logAndThrowRuntime(LOG, e);
            }
        }

        Predicate finalPredicate  = new OrPredicate(allScanPredicates);
        String txnId = getTransactionID();
        Scan scan = SpliceUtils.createScan(txnId);
        EntryPredicateFilter epf = new EntryPredicateFilter(colsToReturn,Arrays.asList(finalPredicate));
        scan.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,epf.toBytes());
        MultiRangeFilter filter = builder.build();
        scan.setStartRow(filter.getMinimumStart());
        scan.setStopRow(filter.getMaximumStop());
        scan.setFilter(filter);

        return scan;
    }

    @Override
    public String toString() {
        return "MultiProbe"+super.toString();
    }

    private class MultiProbeRowProvider extends MultiScanExecRowProvider {
        private final DataValueDescriptor[] probeValues;
        private final byte[] tableName;
        private final SpliceOperation top;

        private HTableInterface table;

        private int currentProbePosition = -1;
        private ResultScanner currentScanner;
        private Scan currentScan;

        private boolean open = false;

        private MultiProbeRowProvider(SpliceOperation top,
                                      RowDecoder rowDecoder,
                                      DataValueDescriptor[] probeValues,
                                      byte[] tableName) {
            super(rowDecoder);
            this.probeValues = probeValues;
            this.tableName = tableName;
            this.top = top;
        }

        @Override
        protected Result getResult() throws IOException {
            Result next = currentScanner.next();
            while(next==null){
                try {
                    nextProbePosition();
                } catch (StandardException e) {
                    SpliceLogUtils.logAndThrow(LOG,new IOException(e));
                }

                // If there are no more rows to probe for, then we're done
                if(currentProbePosition >= probeValues.length) break;

                next = currentScanner.next();
            }
            return next;
        }

        @Override
        public void open() {
            if(table==null)
                table = SpliceAccessManager.getHTable(tableName);
            try {
                nextProbePosition();
            } catch (StandardException e) {
                SpliceLogUtils.logAndThrowRuntime(LOG,e);
            } catch (IOException e) {
                SpliceLogUtils.logAndThrowRuntime(LOG, e);
            }
        }

        @Override
        public void close() {
        	SpliceLogUtils.trace(LOG, "close in MultiProbeRowProvider");
        	if (!isOpen) 
        		return;
            if(currentScanner!=null)currentScanner.close();
            try {
                table.close();
                isOpen = false;
            } catch (IOException e) {
                SpliceLogUtils.logAndThrowRuntime(LOG,e);
            }
        }

        @Override
        public byte[] getTableName() {
            return tableName;
        }

        private void nextProbePosition() throws StandardException, IOException {
            if(currentScanner!=null){
                currentScanner.close(); //close the exhausted scanner, we don't need it anymore
                currentScanner = null;
            }
            currentProbePosition++;
            if(currentProbePosition >= probeValues.length) return; //we're out of rows, just return

            DataValueDescriptor next = probeValues[currentProbePosition];
            currentProbePosition++;
            //skip all the descriptors that match next
            while(currentProbePosition<probeValues.length &&next.equals(probeValues[currentProbePosition])){
                currentProbePosition++;
            }
            currentProbePosition = currentProbePosition-1;
            //get the Scanner for this position
            populateStartAndStopPositions();
            if(startPosition!=null)
                startPosition.getRowArray()[0] = next; //TODO -sf- is this needed?
            if(sameStartStopPosition||stopPosition.getRowArray().length>1){
                stopPosition.getRowArray()[0] = next;
            }
            populateQualifiers();
            currentScan = getScan();
            SpliceUtils.setInstructions(currentScan,activation,top);
            currentScanner = table.getScanner(currentScan);
        }

        @Override
        public List<Scan> getScans() throws StandardException{
            //get the old state so that we can return to it when we're finished
            int oldProbePosition = currentProbePosition;
            Scan oldScan = currentScan;
            ExecIndexRow oldStartPosition = startPosition;
            ExecIndexRow oldStopPosition = stopPosition;

            List<Scan> scans = Lists.newArrayListWithExpectedSize(probeValues.length-oldProbePosition);
            currentProbePosition++;
            if(currentProbePosition >= probeValues.length) return scans; //we're out of rows, just return

            while(currentProbePosition < probeValues.length){
                int probePosition = currentProbePosition;
                scans.add(getNextScan());
                if(probePosition==currentProbePosition)currentProbePosition++;
            }
            //reset to old state
            currentProbePosition = oldProbePosition;
            currentScan = oldScan;
            startPosition = oldStartPosition;
            stopPosition = oldStopPosition;

            populateQualifiers();

            return scans;
        }

        private Scan getNextScan() throws StandardException {
            DataValueDescriptor next = probeValues[currentProbePosition];
            currentProbePosition++;
            //skip all the descriptors that match next
            while(currentProbePosition<probeValues.length &&next.equals(probeValues[currentProbePosition])){
                currentProbePosition++;
            }
            currentProbePosition = currentProbePosition-1;
            //get the Scanner for this position
            populateStartAndStopPositions();
            if(startPosition!=null)
                startPosition.getRowArray()[0] = next;
            if(sameStartStopPosition){
                stopPosition.getRowArray()[0] = next;
            }
            populateQualifiers();
            return getScan();
        }
    }
}
