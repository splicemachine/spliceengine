package com.splicemachine.derby.hbase;

import com.splicemachine.derby.error.SpliceStandardLogUtils;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stats.SinkStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.si2.api.ParentTransactionManager;
import com.splicemachine.utils.SpliceLogUtils;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Callable;

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
	public SinkStats run(final Scan scan, final SpliceObserverInstructions instructions) throws IOException {
        threadLocalEnvironment.set(getEnvironment());
        IOException exception = null;
        final Boolean[] successHolder = new Boolean[] {false};
        final SpliceOperationContext[] contextHolder = new SpliceOperationContext[] {null};
        final HRegion region = ((RegionCoprocessorEnvironment)this.getEnvironment()).getRegion();
        try {
            return ParentTransactionManager.runInParentTransaction(instructions.getTransactionId(), new Callable<SinkStats>() {
                @Override
                public SinkStats call() throws Exception {
                    return runDirect(instructions, scan, contextHolder, region, successHolder);
                }
            });
		} catch (InterruptedException e) {
            exception = new IOException(e);
            throw exception;
        } catch (SQLException e) {
            exception = new IOException(e);
            throw exception;
        } catch (StandardException e) {
        	throw SpliceStandardLogUtils.generateSpliceDoNotRetryIOException(LOG, "run error", e);
		} catch (Exception e) {
            throw SpliceStandardLogUtils.generateSpliceDoNotRetryIOException(LOG, "run error", e);
        } finally {
            threadLocalEnvironment.set(null);
            try{
                if (!successHolder[0] && (contextHolder[0] != null)) {
                    contextHolder[0].close(false);
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

    private SinkStats runDirect(SpliceObserverInstructions instructions, Scan scan,
                                SpliceOperationContext[] contextHolder, HRegion region, Boolean[] successHolder)
            throws SQLException, InterruptedException, StandardException, IOException {
        SpliceLogUtils.trace(LOG, "Running Statement { %s } on operation { %s } with scan { %s }",
                instructions.getStatement(), instructions.getTopOperation(), scan);
        SpliceLogUtils.trace(LOG,"Creating RegionScanner");
        final Connection runningConnection = SpliceDriver.driver().acquireConnection();

        LanguageConnectionContext lcc = runningConnection.unwrap(EmbedConnection.class).getLanguageConnection();
        SpliceUtils.setThreadContext(lcc);
        Activation activation = instructions.getActivation(lcc);

        contextHolder[0] = new SpliceOperationContext(region,scan,
                activation, instructions.getStatement(),runningConnection);
        SpliceOperationRegionScanner spliceScanner = new SpliceOperationRegionScanner(instructions.getTopOperation(),contextHolder[0]);
        SpliceLogUtils.trace(LOG,"performing sink");
        SinkStats out = spliceScanner.sink();
        SpliceLogUtils.trace(LOG, "Coprocessor sunk %d records",out.getSinkStats().getTotalRecords());
        spliceScanner.close();
        successHolder[0] = true;
        return out;
    }

}
