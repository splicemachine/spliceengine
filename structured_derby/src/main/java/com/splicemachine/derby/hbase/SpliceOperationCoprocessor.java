package com.splicemachine.derby.hbase;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.sql.Activation;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
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
	public static byte[] TEMP_TABLE = SpliceConstants.TEMP_TABLE_BYTES;
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
    public TaskStats run(final Scan scan, final SpliceObserverInstructions instructions) throws IOException {
        threadLocalEnvironment.set(getEnvironment());
        IOException exception = null;
        final Boolean[] successHolder = new Boolean[] {false};
        final SpliceOperationContext[] contextHolder = new SpliceOperationContext[] {null};
        final HRegion region = ((RegionCoprocessorEnvironment)this.getEnvironment()).getRegion();
        SpliceTransactionResourceImpl imp = null;
        try {
            try {
                SpliceLogUtils.trace(LOG, "Running Statement { %s } on operation { %s } with scan { %s }",
                        instructions.getStatement(),instructions.getTopOperation(), scan);
                SpliceLogUtils.trace(LOG,"Creating RegionScanner");
                imp = new SpliceTransactionResourceImpl();
                imp.marshallTransaction(instructions);
                Activation activation = instructions.getActivation(imp.getLcc());
                SpliceOperationContext context = new SpliceOperationContext(region,scan,activation, instructions.getStatement(),imp.getLcc(),true,instructions.getTopOperation(),instructions.getSpliceRuntimeContext());
                SpliceOperationRegionScanner spliceScanner = new SpliceOperationRegionScanner(instructions.getTopOperation(),context);
                SpliceLogUtils.trace(LOG,"performing sink");
                TaskStats out = spliceScanner.sink();
                SpliceLogUtils.trace(LOG, "Coprocessor sunk %d records",out.getWriteStats().getTotalRecords());
                spliceScanner.close();
                return out;
            } catch (SQLException e) {
                exception = new IOException(e);
                throw exception;
            } catch (Exception e) {
                SpliceLogUtils.logAndThrow(LOG,"run error", Exceptions.getIOException(e));
                return null;
            } finally {
                threadLocalEnvironment.set(null);
                try{
                    if (!successHolder[0] && (contextHolder[0] != null)) {
                        contextHolder[0].close();
                    }
                }catch(Throwable e){
                    if (exception == null) {
                        throw new IOException(e);
                    } else {
                        throw exception;
                    }
                }
            }
        } finally {
            if (imp != null) {
                imp.cleanup();
            }
        }
    }
}
