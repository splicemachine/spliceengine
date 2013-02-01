package com.splicemachine.derby.hbase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.utils.Puts;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.impl.sql.GenericActivationHolder;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;


public class SpliceOperationRegionScanner implements RegionScanner {
	private static Logger LOG = Logger.getLogger(SpliceOperationRegionScanner.class);
	protected GenericStorablePreparedStatement statement;
	protected SpliceOperation topOperation;
	protected RegionScanner regionScanner;
	protected Iterator<ExecRow> currentRows;
	protected List<KeyValue> currentResult;
	protected Activation activation; // has to be passed by reference... jl
	
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
		}catch (IOException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		}
	}

	public SpliceOperationRegionScanner(RegionScanner regionScanner, Scan scan,HRegion region) {
		SpliceLogUtils.trace(LOG, "instantiated with "+regionScanner+", and scan "+scan);
		this.regionScanner = regionScanner;
		byte[] instructions = scan.getAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS);
		//Putting this here to prevent some kind of weird NullPointer situation
		//where the LanguageConnectionContext doesn't get initialized properly
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(instructions);
			ObjectInputStream ois = new ObjectInputStream(bis);
			SpliceObserverInstructions soi = (SpliceObserverInstructions) ois.readObject();
			statement = soi.getStatement();
			ExecRow[] currentRows = soi.getCurrentRows();
			topOperation = soi.getTopOperation();
			LanguageConnectionContext lcc = SpliceEngine.getLanguageConnectionContext();
			SpliceUtils.setThreadContext();


			activation = ((GenericActivationHolder) statement.getActivation(lcc, false)).ac;
			if(currentRows!=null){
				for(int i=0;i<currentRows.length;i++){
					ExecRow row = currentRows[i];
					if(row!=null){
						activation.setCurrentRow(row,i);
					}
				}
			}
			topOperation.init(new SpliceOperationContext(regionScanner,region,scan, activation, statement, lcc));
			List<SpliceOperation> opStack = new ArrayList<SpliceOperation>();
			topOperation.generateLeftOperationStack(opStack);
			SpliceLogUtils.trace(LOG,"Ready to execute stack %s",opStack);
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "Issues reading serialized data",e);
		}
	}

	@Override
	public boolean next(List<KeyValue> results) throws IOException {
		SpliceLogUtils.trace(LOG, "next ");
		try {
			ExecRow nextRow;
			if ( (nextRow = topOperation.getNextRowCore()) != null) {
				Put put = Puts.buildInsert(nextRow.getRowArray(), null); //todo -sf- add transaction id
				Map<byte[],List<KeyValue>> family = put.getFamilyMap();
				for(byte[] bytes: family.keySet()){
					results.addAll(family.get(bytes));
				}
				SpliceLogUtils.trace(LOG,"next returns results: "+ nextRow);
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
	public long sink() {
		SpliceLogUtils.trace(LOG,"sink");
		return topOperation.sink();
	}
}
