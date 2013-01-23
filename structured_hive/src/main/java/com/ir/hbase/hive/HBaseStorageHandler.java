package com.ir.hbase.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Constants;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.StringUtils;

import com.ir.hbase.client.SchemaManager;
import com.ir.hbase.client.index.IndexTableStructure;
import com.ir.hbase.client.structured.TableStructure;
import com.ir.hbase.hive.index.HBaseDecomposedPredicate;

public class HBaseStorageHandler extends DefaultStorageHandler implements HiveMetaHook, HiveStoragePredicateHandler {
	public static final Log LOG = LogFactory.getLog(HBaseStorageHandler.class);
	final static public String DEFAULT_PREFIX = "default.";
	final static public String INDEX_STRUCTURE = "hbase.index.structure";
	final static public String TABLE_STRUCTURE = "hbase.table.structure";
	private Configuration hbaseConf;
	private HBaseAdmin admin;
	private HBaseAdmin getHBaseAdmin() throws MetaException {
		try {
			if (admin == null) {
				admin = new HBaseAdmin(hbaseConf);
		}
		return admin;
	} catch (MasterNotRunningException mnre) {
		throw new MetaException(StringUtils.stringifyException(mnre));
	} catch (ZooKeeperConnectionException zkce) {
		throw new MetaException(StringUtils.stringifyException(zkce));
	}
	}

private String getHBaseTableName(Table tbl) {
    String tableName = tbl.getParameters().get(HBaseSerDe.HBASE_TABLE_NAME);
    if (tableName == null) {
      tableName = tbl.getSd().getSerdeInfo().getParameters().get(
        HBaseSerDe.HBASE_TABLE_NAME);
    }
    if (tableName == null) {
      tableName = tbl.getDbName() + "." + tbl.getTableName();
      if (tableName.startsWith(DEFAULT_PREFIX)) {
        tableName = tableName.substring(DEFAULT_PREFIX.length());
      }
    }
    return tableName;
  }

@Override
public void preDropTable(Table table) throws MetaException {
	if (LOG.isTraceEnabled()) {
		LOG.trace("pre Drop Table called with " + HiveHBaseUtil.gson.toJson(table));
	}
}

@Override
public void rollbackDropTable(Table table) throws MetaException {
	if (LOG.isTraceEnabled()) {
		LOG.trace("rollbackDropTable called with " + HiveHBaseUtil.gson.toJson(table));
	}
}

@Override
public void commitDropTable(Table tbl, boolean deleteData) throws MetaException {
	if (LOG.isTraceEnabled()) {
		LOG.trace("commitDropTable called with " + HiveHBaseUtil.gson.toJson(tbl));
	}
	try {
    String tableName = getHBaseTableName(tbl);
    boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
    if (deleteData && !isExternal) {
      if (getHBaseAdmin().isTableEnabled(tableName))
        getHBaseAdmin().disableTable(tableName);
      getHBaseAdmin().deleteTable(tableName);
    }
  } catch (IOException ie) {
    throw new MetaException(StringUtils.stringifyException(ie));
  }
}

@Override
public void preCreateTable(Table tbl) throws MetaException {
	if (LOG.isTraceEnabled()) {
		LOG.trace("preCreateTable called with " + HiveHBaseUtil.gson.toJson(tbl));
	}
	boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
  if (tbl.getSd().getLocation() != null) {
    throw new MetaException("LOCATION may not be specified for HBase.");
  }  
  TableStructure tableStructure = HiveHBaseUtil.generateTableStructure(tbl);
    
  try {
	  if (!getHBaseAdmin().tableExists(tbl.getTableName())) {
		  if (!isExternal) {
			  getHBaseAdmin().createTable(TableStructure.generateDefaultStructuredData(tableStructure, tbl.getTableName()));
			  SchemaManager sm = new SchemaManager(new HBaseAdmin(HBaseConfiguration.create()));
	        	TableStructure ts2 = sm.getTableStructure(tbl.getTableName());
		  } else {
			  // an external table
			  throw new MetaException("HBase table " + tbl.getTableName() +
					  " doesn't exist while the table is declared as an external table.");
		  }
			  


	  } else {
		  if (!isExternal) {
			  throw new MetaException("Table " + tbl.getTableName() + " already exists"
					  + " within HBase; use CREATE EXTERNAL TABLE instead to"
					  + " register it in Hive.");
		  }
		  // ensure the table is online
		  new HTable(hbaseConf, tbl.getTableName());
	  } 
  } catch (MasterNotRunningException mnre) {
		  throw new MetaException(StringUtils.stringifyException(mnre));
	  } catch (IOException ie) {
		  throw new MetaException(StringUtils.stringifyException(ie));
	  } 
}

@Override
public void rollbackCreateTable(Table table) throws MetaException {
	if (LOG.isTraceEnabled()) {
		LOG.trace("rollbackCreateTable called with " + HiveHBaseUtil.gson.toJson(table));
	}
  boolean isExternal = MetaStoreUtils.isExternalTable(table);
  String tableName = getHBaseTableName(table);
  try {
    if (!isExternal && getHBaseAdmin().tableExists(tableName)) {
      // we have created an HBase table, so we delete it to roll back;
      if (getHBaseAdmin().isTableEnabled(tableName)) {
        getHBaseAdmin().disableTable(tableName);
      }
      getHBaseAdmin().deleteTable(tableName);
    }
  } catch (IOException ie) {
    throw new MetaException(StringUtils.stringifyException(ie));
  }
}

@Override
public void commitCreateTable(Table table) throws MetaException {
	if (LOG.isTraceEnabled()) {
		LOG.trace("commitCreateTable called with " + HiveHBaseUtil.gson.toJson(table));
	}
}

@Override
public Configuration getConf() {
  return hbaseConf;
}

@Override
public void setConf(Configuration conf) {
  hbaseConf = HBaseConfiguration.create(conf);
}

@Override
public Class<? extends InputFormat> getInputFormatClass() {
  return HiveHBaseTableInputFormat.class;
}

@Override
public Class<? extends OutputFormat> getOutputFormatClass() {
  return HiveHBaseTableOutputFormat.class;
}

@Override
public Class<? extends SerDe> getSerDeClass() {
  return HBaseSerDe.class;
}

@Override
public HiveMetaHook getMetaHook() {
  return this;
}

@Override
public void configureTableJobProperties(TableDesc tableDesc,Map<String, String> jobProperties) {
  Properties tableProperties = tableDesc.getProperties();
  String tableName =
    tableProperties.getProperty(HBaseSerDe.HBASE_TABLE_NAME);
  if (tableName == null) {
    tableName =
      tableProperties.getProperty(Constants.META_TABLE_NAME);
    if (tableName.startsWith(DEFAULT_PREFIX)) {
      tableName = tableName.substring(DEFAULT_PREFIX.length());
    }
  }
    jobProperties.put(HBaseSerDe.HBASE_TABLE_NAME, tableName);
    try {
    	SchemaManager sm = new SchemaManager(getHBaseAdmin());
    	IndexTableStructure is = sm.getIndexTableStructure(tableName);
    	TableStructure ts = sm.getTableStructure(tableName);
    	jobProperties.put(INDEX_STRUCTURE, is.toJSon());
    	jobProperties.put(TABLE_STRUCTURE, ts.toJSon());
    } 
    catch (Exception e){
    	e.printStackTrace();
    	throw new RuntimeException("Cannot access schema manager for " + tableName);
    }
}



@Override
public DecomposedPredicate decomposePredicate(JobConf jobConf,Deserializer deserializer,ExprNodeDesc predicate) {
	if (LOG.isTraceEnabled()) {
		LOG.trace("decomposePredicate called ");	
	}	
	String columnNameProperty = jobConf.get(org.apache.hadoop.hive.serde.Constants.LIST_COLUMNS);
		    List<String> columnNames = Arrays.asList(columnNameProperty.split(","));
		    HBaseDecomposedPredicate decomposedPredicate = null;
		    try {
		    	decomposedPredicate = new HBaseDecomposedPredicate(predicate,deserializer.getObjectInspector(),columnNames);
			} catch (SerDeException e) {
				e.printStackTrace();
			}
		    return decomposedPredicate;
}

	
}
