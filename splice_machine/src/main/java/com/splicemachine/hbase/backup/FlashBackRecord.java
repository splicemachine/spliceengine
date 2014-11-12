package com.splicemachine.hbase.backup;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class FlashBackRecord implements InternalTable {
	public static final String CREATE_TABLE = "create table %s.%s (backup_id bigint, backup_transaction_id bigint, " + 
	"backup_timestamp timestamp, backup_status char(1), backup_filesystem varchar(1024))";	
	public static final String INSERT_START_BACKUP = "insert into table %s.%s (backup_transaction_id, backup_timestamp, backup_status, backup_filesystem) values (?,?,?,?)";
	public static final String UPDATE_FINISHED_BACKUP = "update %s.%s set backup_status = ?";
	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}	
	
}
