package com.splicemachine.derby.hbase;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stats.SinkStats;
import com.splicemachine.derby.stats.ThroughputStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;
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
	public static final ThreadLocal<CoprocessorEnvironment> threadLocalEnvironment = new ThreadLocal<CoprocessorEnvironment>();

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
	public SinkStats run(Scan scan,SpliceObserverInstructions instructions) throws IOException {
		threadLocalEnvironment.set(getEnvironment());
		try {
			SpliceLogUtils.trace(LOG, "Running Statement { %s } on operation { %s } with scan { %s }",
																			instructions.getStatement(),instructions.getTopOperation(), scan);
			HRegion region = ((RegionCoprocessorEnvironment)this.getEnvironment()).getRegion();
			SpliceLogUtils.trace(LOG,"Creating RegionScanner");
			LanguageConnectionContext lcc = SpliceDriver.driver().getLanguageConnectionContext();
			SpliceUtils.setThreadContext();
			Activation activation = instructions.getActivation(lcc);

			SpliceOperationContext context = new SpliceOperationContext(region,scan, activation, instructions.getStatement(),lcc);
			SpliceOperationRegionScanner spliceScanner = new SpliceOperationRegionScanner(instructions.getTopOperation(),context);
			SpliceLogUtils.trace(LOG,"performing sink");
			SinkStats out = spliceScanner.sink();
			SpliceLogUtils.trace(LOG, "Coprocessor sunk %d records",out.getSinkStats().getTotalRecords());
			spliceScanner.close();
			return out;
		} finally {
			threadLocalEnvironment.set(null);
		}
	}

}
