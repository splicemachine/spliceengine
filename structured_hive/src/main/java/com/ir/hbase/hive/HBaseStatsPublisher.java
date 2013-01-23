package com.ir.hbase.hive;

import java.io.IOException;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;

public class HBaseStatsPublisher implements StatsPublisher {

	  private HTable htable;
	  private final Log LOG = LogFactory.getLog(this.getClass().getName());

	  /**
	   * Does the necessary HBase initializations.
	   */
	  public boolean connect(Configuration hiveconf) {

	    try {
	      Configuration hbaseConf = HBaseConfiguration.create(hiveconf);
	      htable = new HTable(hbaseConf, HBaseStatsSetupConstants.PART_STAT_TABLE_NAME);
	      // for performance reason, defer update until the closeConnection
	      htable.setAutoFlush(false);
	    } catch (IOException e) {
	      LOG.error("Error during HBase connection. " + e);
	      return false;
	    }

	    return true;
	  }

	  /**
	   * Writes temporary statistics into HBase;
	   */
	  public boolean publishStat(String rowID, Map<String, String> stats) {

	    // Write in HBase

	    if (!HBaseStatsUtils.isValidStatisticSet(stats.keySet())) {
	      LOG.warn("Warning. Invalid statistic: " + stats.keySet().toString()
	          + ", supported stats: "
	          + HBaseStatsUtils.getSupportedStatistics());
	      return false;
	    }

	    try {

	      // check the basic stat (e.g., row_count)

	      Get get = new Get(Bytes.toBytes(rowID));
	      Result result = htable.get(get);

	      byte[] family = HBaseStatsUtils.getFamilyName();
	      byte[] column = HBaseStatsUtils.getColumnName(HBaseStatsUtils.getBasicStat());

	      long val = Long.parseLong(HBaseStatsUtils.getStatFromMap(HBaseStatsUtils.getBasicStat(),
	          stats));
	      long oldVal = 0;

	      if (!result.isEmpty()) {
	        oldVal = Long.parseLong(Bytes.toString(result.getValue(family, column)));
	      }

	      if (oldVal >= val) {
	        return true; // we do not need to publish anything
	      }

	      // we need to update
	      Put row = new Put(Bytes.toBytes(rowID));
	      for (String statType : HBaseStatsUtils.getSupportedStatistics()) {
	        column = HBaseStatsUtils.getColumnName(statType);
	        row.add(family, column, Bytes.toBytes(HBaseStatsUtils.getStatFromMap(statType, stats)));
	      }

	      htable.put(row);
	      return true;

	    } catch (IOException e) {
	      LOG.error("Error during publishing statistics. " + e);
	      return false;
	    }
	  }

	  public boolean closeConnection() {
	    // batch update
	    try {
	      htable.flushCommits();
	      return true;
	    } catch (IOException e) {
	      LOG.error("Cannot commit changes in stats publishing.", e);
	      return false;
	    }
	  }


	  /**
	   * Does the necessary HBase initializations.
	   */
	  public boolean init(Configuration hiveconf) {
	    try {
	      Configuration hbaseConf = HBaseConfiguration.create(hiveconf);
	      HBaseAdmin hbase = new HBaseAdmin(hbaseConf);

	      // Creating table if not exists
	      if (!hbase.tableExists(HBaseStatsSetupConstants.PART_STAT_TABLE_NAME)) {
	        HTableDescriptor table = new HTableDescriptor(HBaseStatsSetupConstants.PART_STAT_TABLE_NAME);
	        HColumnDescriptor family = new HColumnDescriptor(HBaseStatsUtils.getFamilyName());
	        table.addFamily(family);
	        hbase.createTable(table);
	      }
	    } catch (IOException e) {
	      LOG.error("Error during HBase initialization. " + e);
	      return false;
	    }

	    return true;
	  }
	}