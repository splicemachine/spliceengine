package com.ir.hbase.hive.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Constants;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.index.IndexPredicateAnalyzer;
import org.apache.hadoop.hive.ql.index.IndexSearchCondition;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.util.StringUtils;
import com.ir.constants.HBaseConstants;
import com.ir.hbase.client.structured.Column;
import com.ir.hbase.client.structured.Family;
import com.ir.hbase.client.structured.TableStructure;

public class HBaseIndexStorageHandler extends DefaultStorageHandler
implements HiveMetaHook, HiveStoragePredicateHandler {
final static public String DEFAULT_PREFIX = "default.";
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
@Override
public DecomposedPredicate decomposePredicate(JobConf jobConf,
		Deserializer deserializer, ExprNodeDesc predicate) {
	return null;
}
@Override
public void preCreateTable(Table table) throws MetaException {
	
}
@Override
public void rollbackCreateTable(Table table) throws MetaException {
	
}
@Override
public void commitCreateTable(Table table) throws MetaException {
}
@Override
public void preDropTable(Table table) throws MetaException {
}
@Override
public void rollbackDropTable(Table table) throws MetaException {
	
}
@Override
public void commitDropTable(Table table, boolean deleteData)
		throws MetaException {
	
}

@Override
public HiveMetaHook getMetaHook() {
  // no hook by default
  return this;
}



}
