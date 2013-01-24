package com.splicemachine.derby.impl.store.access;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

public class SpliceHTableFactory implements HTableInterfaceFactory {
	private static Logger LOG = Logger.getLogger(SpliceHTableFactory.class);
	private boolean autoFlush = true;

	public SpliceHTableFactory() {
		SpliceLogUtils.trace(LOG, "instantiated with autoFlush set to %s",autoFlush);
	}
	
	public SpliceHTableFactory(boolean autoFlush) {
		SpliceLogUtils.trace(LOG, "instantiated with autoFlush set to %s",autoFlush);
		this.autoFlush = autoFlush;
	}
	
	  @Override
	  public HTableInterface createHTableInterface(Configuration config,byte[] tableName) {
			SpliceLogUtils.trace(LOG, "createHTableInterface for %s",Bytes.toString(tableName));
	    try {
	    	HTable htable = new HTable(config, tableName);
	    	htable.setAutoFlush(autoFlush);
	    	return htable;
	    } catch (IOException ioe) {
	      throw new RuntimeException(ioe);
	    }
	  }

	  @Override
	  public void releaseHTableInterface(HTableInterface table) throws IOException {
		SpliceLogUtils.trace(LOG, "releaseHTableInterface for %s",Bytes.toString(table.getTableName()));
	    table.close();
	  }
	}