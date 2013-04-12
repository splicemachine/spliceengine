package com.splicemachine.derby.hbase;

import com.splicemachine.derby.error.SpliceStandardException;
import com.splicemachine.derby.error.SpliceStandardLogUtils;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stats.SinkStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.si2.txn.TransactionManager;
import com.splicemachine.utils.SpliceLogUtils;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stats.SinkStats;
import com.splicemachine.derby.utils.SpliceUtils;
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
        Connection runningConnection = null;
        final String oldParentTransactionId = TransactionManager.getParentTransactionId();
        IOException exception = null;
        boolean success = false;
        SpliceOperationContext context = null;
        try {
            TransactionManager.setParentTransactionId(instructions.getTransactionId());

			SpliceLogUtils.trace(LOG, "Running Statement { %s } on operation { %s } with scan { %s }",
																			instructions.getStatement(),instructions.getTopOperation(), scan);
			HRegion region = ((RegionCoprocessorEnvironment)this.getEnvironment()).getRegion();
			SpliceLogUtils.trace(LOG,"Creating RegionScanner");
            runningConnection = SpliceDriver.driver().acquireConnection();

			LanguageConnectionContext lcc = runningConnection.unwrap(EmbedConnection.class).getLanguageConnection();
			SpliceUtils.setThreadContext(lcc);
			Activation activation = instructions.getActivation(lcc);

			context = new SpliceOperationContext(region,scan,
                    activation, instructions.getStatement(),runningConnection);
			SpliceOperationRegionScanner spliceScanner = new SpliceOperationRegionScanner(instructions.getTopOperation(),context);
			SpliceLogUtils.trace(LOG,"performing sink");
			SinkStats out = spliceScanner.sink();
			SpliceLogUtils.trace(LOG, "Coprocessor sunk %d records",out.getSinkStats().getTotalRecords());
			spliceScanner.close();
            success = true;
			return out;
		} catch (InterruptedException e) {
            exception = new IOException(e);
            throw exception;
        } catch (SQLException e) {
            exception = new IOException(e);
            throw exception;
        } catch (StandardException e) {
        	throw SpliceStandardLogUtils.generateSpliceDoNotRetryIOException(LOG, "run error", e);
		} finally {
            TransactionManager.setParentTransactionId(oldParentTransactionId);
            threadLocalEnvironment.set(null);
            try{
                if (!success && (context != null)) {
                    context.close(false);
                }
            }catch(Throwable e){
                if (exception == null) {
                    throw new IOException(e);
                } else {
                    throw exception;
                }
            }
		}
	}

}
