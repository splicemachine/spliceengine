
package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.MultiProbeClientScanProvider;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.compile.RowOrdering;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;

// These are for javadoc "@see" tags.

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
        this.sortRequired = sortRequired;
        if(this.sortRequired!=RowOrdering.DONTCARE)
            sortProbeValues();
        this.scanInformation = new MultiProbeDerbyScanInformation(
                resultRowAllocator.getMethodName(),
                startKeyGetter==null?null:startKeyGetter.getMethodName(),
                stopKeyGetter==null?null:stopKeyGetter.getMethodName(),
                qualifiersField==null?null:qualifiersField,
                conglomId,
                colRefItem,
                sameStartStopPosition,
                startSearchOperator,
                stopSearchOperator,
                probingVals
        );
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
    public String toString() {
        return "MultiProbe"+super.toString();
    }
    
	@Override
	public RowProvider getMapRowProvider(SpliceOperation top,PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
		beginTime = System.currentTimeMillis();
			List<Scan> scans = scanInformation.getScans(getTransactionID(), null, activation, top, spliceRuntimeContext);
			for(Scan scan:scans){
					//remove SI behavior from scan to make sure that we do it ourselves
					deSiify(scan);
			}
			MultiProbeClientScanProvider provider = new MultiProbeClientScanProvider("tableScan", TableName.valueOf(tableName),
							scans, decoder,spliceRuntimeContext);
		nextTime += System.currentTimeMillis() - beginTime;
		return provider;
	}

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException {
        return getMapRowProvider(top, decoder, spliceRuntimeContext);
    }
        
}
