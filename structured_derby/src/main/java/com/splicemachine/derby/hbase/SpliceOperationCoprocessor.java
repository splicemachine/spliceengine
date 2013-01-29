package com.splicemachine.derby.hbase;

import java.io.IOException;
import java.util.Properties;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.utils.SpliceUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.impl.sql.GenericActivationHolder;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.utils.SpliceLogUtils;
/**
 * 
 * HBase Endpoint coprocessor handling sink operations from the OperationTree.
 * 
 * @author johnleach
 *
 */
public class SpliceOperationCoprocessor extends BaseEndpointCoprocessor implements SpliceOperationProtocol{
	private static Logger LOG = Logger.getLogger(SpliceOperationCoprocessor.class);
	public static String TEMP_TABLE_STR = "SYS_TEMP";
	public static byte[] TEMP_TABLE = Bytes.toBytes(TEMP_TABLE_STR);
	protected static ContextManager contextManager;
	static {
		Monitor.startMonitor(new Properties(), null);
		Monitor.clearMonitor();
	}
	/**
	 * Start the hbase coprocessor (empty)
	 * 
	 */
	@Override
	public void start(CoprocessorEnvironment env) {
		SpliceLogUtils.info(LOG, "starting coprocessor");
		super.start(env);
	}

	/**
	 * Stop the hbase coprocessor (empty)
	 * 
	 */
	@Override
	public void stop(CoprocessorEnvironment env) {
		SpliceLogUtils.info(LOG, "stopping coprocessor");
		super.stop(env);
	}
	/**
	 * The method that exeucutes the endpoint for the specific region.  It wraps the regions scanner in a SpliceOperationRegionScanner for
	 * execution and then calls the SpliceOperationRegionScanner.sink method.
	 * 
	 */
	@Override
	public long run(GenericStorablePreparedStatement statement, Scan scan, SpliceOperation topOperation) throws IOException,StandardException {
		SpliceLogUtils.trace(LOG, "Running Statement { %s } on operation { %s } with scan { %s }",statement.toString(),topOperation, scan);
		HRegion region = ((RegionCoprocessorEnvironment)this.getEnvironment()).getRegion();
		SpliceLogUtils.trace(LOG,"Creating RegionScanner");
		LanguageConnectionContext lcc = SpliceEngine.getLanguageConnectionContext();
		SpliceUtils.setThreadContext();
		Activation activation = ((GenericActivationHolder)statement.getActivation(lcc,false)).ac;

		SpliceOperationContext context = new SpliceOperationContext(region,scan, activation, statement,lcc);
		SpliceOperationRegionScanner spliceScanner = new SpliceOperationRegionScanner(topOperation,context);
		SpliceLogUtils.trace(LOG,"performing sink");
		long out = spliceScanner.sink();
		SpliceLogUtils.trace(LOG, "Coprocessor sunk %d records",out);
		spliceScanner.close();
		return out;
	}

}
