package com.ir.hbase.hive.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.index.HiveIndexHandler;
import org.apache.hadoop.hive.ql.index.HiveIndexQueryContext;
import org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

import com.google.gson.Gson;
import com.ir.hbase.client.SchemaManager;
import com.ir.hbase.client.index.IndexColumn;
import com.ir.hbase.client.index.IndexColumn.Order;
import com.ir.hbase.client.index.IndexTableStructure;
import com.ir.hbase.client.structured.Column;
import com.ir.hbase.client.structured.Column.Type;
import com.ir.hbase.client.structured.TableStructure;
import com.ir.hbase.index.mapreduce.DictionaryMapReduceUtil;

public class HBaseIndexHandler implements HiveIndexHandler {
	private Configuration conf = HBaseConfiguration.create();
	
	@Override
	public Configuration getConf() {
		// TODO Auto-generated method stub
		CompactIndexHandler handler;
		return conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = HBaseConfiguration.create(conf);
	}

	@Override
	public boolean usesIndexTable() {
		return false;
	}

	@Override
	public void analyzeIndexDefinition(Table baseTable, Index index, Table indexTable) throws HiveException {
		System.out.println("Analyze Index Definition");
		try {
			SchemaManager sm = new SchemaManager(new HBaseAdmin(HBaseConfiguration.create()));
			TableStructure ts = sm.getTableStructure(baseTable.getTableName());
			  System.out.println(baseTable.getTableName() + ts.toJSon());
			com.ir.hbase.client.index.Index irIndex = new com.ir.hbase.client.index.Index(index.getIndexName());
			System.out.println(ts.toJSon());
			System.out.println("Analyze Index Definition a");
			for (FieldSchema fieldSchema : index.getSd().getCols()) {
				System.out.println("Analyze Index Definition b " + fieldSchema.getName());
				Column column = ts.getColumn(fieldSchema.getName());
				System.out.println("Analyze Index Definition c " + column);
				System.out.println("Analyze Index Definition c.1 " + column.getFamily());
				System.out.println("Analyze Index Definition c .2" + column.getColumnName());
				System.out.println("Analyze Index Definition c .3" + column.getType());
				
				
				IndexColumn indexColumn = new IndexColumn(column.getFamily(),column.getColumnName(),Order.ASCENDING,column.getType());
				System.out.println("Analyze Index Definition d");
				irIndex.addIndexColumn(indexColumn);
			}
		System.out.println("Analyze Index Definition 2");
		sm.addIndexToTable(baseTable.getTableName(), irIndex);	
		try {
			DictionaryMapReduceUtil.addIndex(baseTable.getTableName().getBytes(), irIndex, DictionaryMapReduceUtil.MIN_TIMESTAMP, conf);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Analyze Index Definition 3");
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public List<Task<?>> generateIndexBuildTaskList(org.apache.hadoop.hive.ql.metadata.Table baseTbl, Index index,List<Partition> indexTblPartitions,
			List<Partition> baseTblPartitions,
			org.apache.hadoop.hive.ql.metadata.Table indexTbl,
			Set<ReadEntity> inputs, Set<WriteEntity> outputs)
			throws HiveException {		
		return new ArrayList<Task<?>>();
	}

	@Override
	public void generateIndexQuery(List<Index> indexes, ExprNodeDesc predicate,ParseContext pctx, HiveIndexQueryContext queryContext) {

	}

	@Override
	public boolean checkQuerySize(long inputSize, HiveConf conf) {
		return false;
	}

}
