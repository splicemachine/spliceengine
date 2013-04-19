package com.splicemachine.derby.hbase;

import com.splicemachine.derby.error.SpliceStandardLogUtils;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.stats.TimeUtils;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.si.api.ParentTransactionManager;
import com.splicemachine.utils.SpliceLogUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.log4j.Logger;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;


public class SpliceOperationRegionScanner implements RegionScanner {
    private static Logger LOG = Logger.getLogger(SpliceOperationRegionScanner.class);
    protected GenericStorablePreparedStatement statement;
    protected SpliceOperation topOperation;
    protected RegionScanner regionScanner;
    protected Iterator<ExecRow> currentRows;
    protected List<KeyValue> currentResult;
    protected Activation activation; // has to be passed by reference... jl
    private String parentTransactionId;
    private Serializer serializer = new Serializer();

    private TaskStats.SinkAccumulator stats = TaskStats.uniformAccumulator();
    private TaskStats finalStats;
    private SpliceOperationContext context;

    public SpliceOperationRegionScanner(SpliceOperation topOperation,
                                        SpliceOperationContext context) throws StandardException {
    	stats.start();
    	SpliceLogUtils.trace(LOG, ">>>>statistics starts for SpliceOperationRegionScanner at "+stats.getStartTime());
        this.topOperation = topOperation;
        this.statement = context.getPreparedStatement();
        this.context = context;
        try {
            this.regionScanner = context.getScanner();

            activation = context.getActivation();//((GenericActivationHolder) statement.getActivation(lcc, false)).ac;
            topOperation.init(context);
        }catch (IOException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, e);
        }
    }

	public SpliceOperationRegionScanner(final RegionScanner regionScanner, final Scan scan, final HRegion region) {
		SpliceLogUtils.trace(LOG, "instantiated with "+regionScanner+", and scan "+scan);
		stats.start();
		SpliceLogUtils.trace(LOG, ">>>>statistics starts for SpliceOperationRegionScanner at "+stats.getStartTime());
		this.regionScanner = regionScanner;

        try {
			SpliceObserverInstructions soi = SpliceUtils.getSpliceObserverInstructions(scan);
	        statement = soi.getStatement();
	        topOperation = soi.getTopOperation();
	        SpliceTransactionResourceImpl impl = new SpliceTransactionResourceImpl();
	        impl.marshallTransaction(soi.getTransactionId());
	        activation = soi.getActivation(impl.getLcc());
	        context = new SpliceOperationContext(regionScanner,region,scan, activation, statement, impl.getLcc());
	        topOperation.init(context);
	        List<SpliceOperation> opStack = new ArrayList<SpliceOperation>();
	        topOperation.generateLeftOperationStack(opStack);
	        SpliceLogUtils.trace(LOG, "Ready to execute stack %s", opStack);
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "Issues reading serialized data",e);
        }
	}



    @Override
	public boolean next(final List<KeyValue> results) throws IOException {
		SpliceLogUtils.trace(LOG, "next ");
		try {
			ExecRow nextRow;
	        long start = System.nanoTime();
	        if ( (nextRow = topOperation.getNextRowCore()) != null) {
	            stats.readAccumulator().tick(System.nanoTime()-start);
	            start = System.nanoTime();
	            Put put = Puts.buildInsert(nextRow.getRowArray(), SpliceUtils.NA_TRANSACTION_ID,serializer); //todo -sf- add transaction id
	            Map<byte[],List<KeyValue>> family = put.getFamilyMap();
	            for(byte[] bytes: family.keySet()){
	                results.addAll(family.get(bytes));
	            }
	            SpliceLogUtils.trace(LOG,"next returns results: "+ nextRow);
	            stats.writeAccumulator().tick(System.nanoTime()-start);
	            return true;
	        }
	        return false;
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,"error during next call: ",e);
        }
        return false;
	}



    @Override
	public boolean next(List<KeyValue> result, int limit) throws IOException {
		throw new RuntimeException("Not Implemented");
	}

	@Override
	public void close() throws IOException {
        SpliceLogUtils.trace(LOG, "close");
        boolean success = false;
        try {
            topOperation.close();
            success = true;
        } catch (StandardException e) {
        	SpliceStandardLogUtils.generateSpliceDoNotRetryIOException(LOG, "close direct failed", e);
        }finally{
            if (regionScanner != null) {
                regionScanner.close();
            }
            finalStats = stats.finish();
            ((SpliceBaseOperation)topOperation).nextTime +=finalStats.getTotalTime();
            SpliceLogUtils.trace(LOG, ">>>>statistics finishes for sink for SpliceOperationRegionScanner at "+stats.getFinishTime());
            context.close(success);
        }
    }

    @Override
	public HRegionInfo getRegionInfo() {
		SpliceLogUtils.trace(LOG,"getRegionInfo");
		return regionScanner.getRegionInfo();
	}

	@Override
	public boolean isFilterDone() {
		SpliceLogUtils.trace(LOG,"isFilterDone");
		return regionScanner.isFilterDone();
	}

	public TaskStats sink() throws IOException{
		SpliceLogUtils.trace(LOG,"sink");
		return topOperation.sink();
	}

    public void reportMetrics() {
        //Report statistics with the top operation logger
        Logger logger = Logger.getLogger(topOperation.getClass());

        if(!logger.isDebugEnabled()) return; //no stats should be printed

        StringBuilder summaryBuilder = new StringBuilder()
                .append("Scanner Time: ").append(TimeUtils.toSeconds(finalStats.getTotalTime()))
                .append("\t").append("Region name: ").append(regionScanner.getRegionInfo().getRegionNameAsString())
                .append("\n")
                .append("ProcessStats:\n")
                .append("\t").append(finalStats.getReadStats())
                .append("\nWriteStats:\n")
                .append("\t").append(finalStats.getWriteStats());
        logger.debug(summaryBuilder.toString());
    }
}
