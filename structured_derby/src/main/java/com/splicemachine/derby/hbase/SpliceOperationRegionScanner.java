package com.splicemachine.derby.hbase;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.stats.Accumulator;
import com.splicemachine.derby.stats.SinkStats;
import com.splicemachine.derby.stats.ThroughputStats;
import com.splicemachine.derby.stats.TimeUtils;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class SpliceOperationRegionScanner implements RegionScanner {
    private static Logger LOG = Logger.getLogger(SpliceOperationRegionScanner.class);
    protected GenericStorablePreparedStatement statement;
    protected SpliceOperation topOperation;
    protected RegionScanner regionScanner;
    protected Iterator<ExecRow> currentRows;
    protected List<KeyValue> currentResult;
    protected Activation activation; // has to be passed by reference... jl
    private Serializer serializer = new Serializer();

    private SinkStats.SinkAccumulator stats = SinkStats.uniformAccumulator();
    private SinkStats finalStats;

    public SpliceOperationRegionScanner(SpliceOperation topOperation,
                                        SpliceOperationContext context){
        this.topOperation = topOperation;
        this.statement = context.getPreparedStatement();
        try {
            this.regionScanner = context.getScanner();
            LanguageConnectionContext lcc = context.getLanguageConnectionContext();
            SpliceLogUtils.trace(LOG,"lcc=%s",lcc);
            activation = context.getActivation();//((GenericActivationHolder) statement.getActivation(lcc, false)).ac;
            topOperation.init(context);

            stats.start();
        }catch (IOException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG,e);
        }
    }

	public SpliceOperationRegionScanner(RegionScanner regionScanner, Scan scan,HRegion region) {
		SpliceLogUtils.trace(LOG, "instantiated with "+regionScanner+", and scan "+scan);
		this.regionScanner = regionScanner;
		//Putting this here to prevent some kind of weird NullPointer situation
		//where the LanguageConnectionContext doesn't get initialized properly
		try {
			SpliceObserverInstructions soi = SpliceUtils.getSpliceObserverInstructions(scan);
			statement = soi.getStatement();
			topOperation = soi.getTopOperation();
			LanguageConnectionContext lcc = SpliceDriver.driver().getLanguageConnectionContext();
			SpliceUtils.setThreadContext();

			activation = soi.getActivation(lcc);

			topOperation.init(new SpliceOperationContext(regionScanner,region,scan, activation, statement, lcc));
			List<SpliceOperation> opStack = new ArrayList<SpliceOperation>();
			topOperation.generateLeftOperationStack(opStack);
			SpliceLogUtils.trace(LOG,"Ready to execute stack %s",opStack);
            stats.start();
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "Issues reading serialized data",e);
		}
	}

	@Override
	public boolean next(List<KeyValue> results) throws IOException {
		SpliceLogUtils.trace(LOG, "next ");
		try {
			ExecRow nextRow;
            long start = System.nanoTime();
			if ( (nextRow = topOperation.getNextRowCore()) != null) {
                stats.processAccumulator().tick(System.nanoTime()-start);

                start = System.nanoTime();
				Put put = Puts.buildInsert(nextRow.getRowArray(), null,serializer); //todo -sf- add transaction id
				Map<byte[],List<KeyValue>> family = put.getFamilyMap();
				for(byte[] bytes: family.keySet()){
					results.addAll(family.get(bytes));
				}
				SpliceLogUtils.trace(LOG,"next returns results: "+ nextRow);
                stats.sinkAccumulator().tick(System.nanoTime()-start);
				return true;
			}
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
		try {
			topOperation.close();
		} catch (StandardException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		}
		if (regionScanner != null)
			regionScanner.close();
        finalStats = stats.finish();
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

	public SinkStats sink() throws IOException{
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
                .append("\t").append(finalStats.getProcessStats())
                .append("\nWriteStats:\n")
                .append("\t").append(finalStats.getSinkStats());
        logger.debug(summaryBuilder.toString());
    }
}
