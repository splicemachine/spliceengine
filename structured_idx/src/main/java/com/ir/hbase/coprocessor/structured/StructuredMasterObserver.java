package com.ir.hbase.coprocessor.structured;

import java.io.IOException;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import com.ir.hbase.client.structured.TableStructure;
import com.ir.hbase.coprocessor.BaseZkMasterObserver;
import com.ir.hbase.coprocessor.SchemaUtils;
import com.ir.hbase.coprocessor.index.IndexMasterObserver;
import com.ir.hbase.coprocessor.index.IndexUtils;
import com.ir.constants.SchemaConstants;
import com.ir.constants.TxnConstants;
import com.ir.constants.TxnConstants.TableEnv;
import com.ir.constants.environment.EnvUtils;

public class StructuredMasterObserver extends BaseZkMasterObserver {
	private static Logger LOG = Logger.getLogger(StructuredMasterObserver.class);

	
	
	@Override
	public void start(CoprocessorEnvironment ctx) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("Starting the CoProcessor " + StructuredMasterObserver.class);
		if (LOG.isTraceEnabled())
			LOG.trace("Starting the CoProcessor");
		initZooKeeper(ctx);
		super.start(ctx);
	}

	@Override
	public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("preCreateTable in CoProcessor " + StructuredMasterObserver.class);
		if (!EnvUtils.getTableEnv(desc.getNameAsString()).equals(TxnConstants.TableEnv.OTHER_TABLES)) {
			ctx.complete();
			return;
		}
		String tableName = desc.getNameAsString();
		TableStructure ts = TableStructure.getTableStructure(desc);
		TableStructure.removeTableStructure(desc);
		StructuredUtils.createStructureNodes(ts, schemaPath, tableName, desc, rzk);
		super.preCreateTable(ctx, desc, regions);
	}
	@Override
	public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("Server side Schema Editor in preDeleteTable in CoProcessor " + IndexMasterObserver.class);
		if (EnvUtils.getTableEnv(Bytes.toString(tableName)).equals(TxnConstants.TableEnv.OTHER_TABLES)) {
			StructuredUtils.deleteStructuredTablePathRecursively(schemaPath, Bytes.toString(tableName), zkw);
		}
		super.preDeleteTable(ctx, tableName);
	}

	@Override
	public void stop(CoprocessorEnvironment ctx) throws IOException {
		if (LOG.isTraceEnabled())
			LOG.trace("Stopping the CoProcessor " + StructuredMasterObserver.class);
		super.stop(ctx);
	}
}
