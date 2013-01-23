package com.ir.hbase.coprocessor.index;

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

import com.ir.hbase.client.index.IndexTableStructure;
import com.ir.hbase.coprocessor.BaseZkMasterObserver;
import com.ir.hbase.coprocessor.SchemaUtils;
import com.ir.hbase.coprocessor.structured.StructuredMasterObserver;
import com.ir.constants.SchemaConstants;
import com.ir.constants.TxnConstants;
import com.ir.constants.TxnConstants.TableEnv;
import com.ir.constants.environment.EnvUtils;

public class IndexMasterObserver extends BaseZkMasterObserver {
	private static Logger LOG = Logger.getLogger(IndexMasterObserver.class);

	@Override
	public void start(CoprocessorEnvironment ctx) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("Starting the CoProcessor " + IndexMasterObserver.class);
		if (LOG.isTraceEnabled())
			LOG.trace("Starting the CoProcessor");
		initZooKeeper(ctx);
		super.start(ctx);
	}

	@Override
	public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("preCreateTable in CoProcessor " + IndexMasterObserver.class);
		if (!EnvUtils.getTableEnv(desc.getNameAsString()).equals(TxnConstants.TableEnv.OTHER_TABLES)) {
			ctx.complete();
			return;
		}
		String tableName = desc.getNameAsString();
		IndexTableStructure its = IndexTableStructure.getIndexTableStructure(desc);
		IndexTableStructure.removeIndexTableStructure(desc);
		IndexUtils.createIndexTableAndNodes(its, schemaPath, tableName, desc, rzk, ctx);
		super.preCreateTable(ctx, desc, regions);
	}
	@Override
	public void stop(CoprocessorEnvironment ctx) throws IOException {
		if (LOG.isTraceEnabled())
			LOG.trace("Stopping the CoProcessor " + IndexMasterObserver.class);
		super.stop(ctx);
	}
	@Override
	public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("Server side Schema Editor in preDeleteTable in CoProcessor " + IndexMasterObserver.class);
		String tableNameStr = Bytes.toString(tableName);
		if (EnvUtils.getTableEnv(tableNameStr).equals(TableEnv.OTHER_TABLES) ) {
			HBaseAdmin admin = new HBaseAdmin(ctx.getEnvironment().getConfiguration());
			if (admin.tableExists(SchemaUtils.appendSuffix(tableNameStr, SchemaConstants.INDEX))) {
				if (!admin.isTableDisabled(SchemaUtils.appendSuffix(tableNameStr, SchemaConstants.INDEX)))
					admin.disableTable(SchemaUtils.appendSuffix(tableNameStr, SchemaConstants.INDEX));
				admin.deleteTable(SchemaUtils.appendSuffix(tableNameStr, SchemaConstants.INDEX));
			}
			IndexUtils.deleteIdxTablePathRecursively(schemaPath, tableNameStr, zkw);
		}
		super.preDeleteTable(ctx, tableName);
	}

	@Override
	public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("Server side Schema Editor in preEnableTable in CoProcessor " + IndexMasterObserver.class);
		String tableNameStr = Bytes.toString(tableName);
		if (EnvUtils.getTableEnv(tableNameStr).equals(TableEnv.OTHER_TABLES) ) {
			HBaseAdmin admin = new HBaseAdmin(ctx.getEnvironment().getConfiguration());
			if (admin.tableExists(SchemaUtils.appendSuffix(tableNameStr, SchemaConstants.INDEX))) {
				admin.enableTable(Bytes.toBytes(SchemaUtils.appendSuffix(tableNameStr, SchemaConstants.INDEX)));
			}
		}		
		super.preEnableTable(ctx, tableName);
	}

	@Override
	public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName) throws IOException {
		if (LOG.isDebugEnabled())
			LOG.debug("Server side Schema Editor in preDisableTable in CoProcessor " + IndexMasterObserver.class);
		String tableNameStr = Bytes.toString(tableName);
		if (EnvUtils.getTableEnv(tableNameStr).equals(TableEnv.OTHER_TABLES) ) {
			HBaseAdmin admin = new HBaseAdmin(ctx.getEnvironment().getConfiguration());			
			if (admin.tableExists(SchemaUtils.appendSuffix(tableNameStr, SchemaConstants.INDEX))) {
				admin.disableTable(Bytes.toBytes(SchemaUtils.appendSuffix(tableNameStr, SchemaConstants.INDEX)));
			}
		}		
		super.preDisableTable(ctx, tableName);
	}
	
	

}
