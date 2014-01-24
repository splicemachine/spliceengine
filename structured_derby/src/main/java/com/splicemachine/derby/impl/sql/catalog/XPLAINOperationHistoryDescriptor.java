package com.splicemachine.derby.impl.sql.catalog;

import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.impl.sql.catalog.SystemColumnImpl;

import java.sql.Types;

/**
 * @author Scott Fines
 * Date: 1/21/14
 */
public class XPLAINOperationHistoryDescriptor extends XPLAINTaskDescriptor{
		private static final String TABLE_NAME = "SYSXPLAIN_OPERATIONHISTORY";
		private static final String STATEMENTID_NAME = "STATEMENTID";
		private static final String OPERATIONID_NAME = "OPERATIONID";

		@Override public String getTableName() { return TABLE_NAME; }

		@Override
		protected String[] getPrimaryKeys() {
				return new String[]{STATEMENTID_NAME,OPERATIONID_NAME};
		}

		@Override
		protected SystemColumn[] buildColumnList() {
				return new SystemColumn[]{
								SystemColumnImpl.getColumn(STATEMENTID_NAME, Types.BIGINT,false),
								SystemColumnImpl.getColumn(OPERATIONID_NAME,Types.BIGINT,false),
								SystemColumnImpl.getColumn("OPERATION_TYPE",Types.VARCHAR,false,32642),
								SystemColumnImpl.getColumn("PARENT_OPERATION_ID",Types.BIGINT,true),
								SystemColumnImpl.getColumn("IS_RIGHT_CHILD_OP",Types.BOOLEAN,false),
								SystemColumnImpl.getColumn("IS_SINK",Types.BOOLEAN,false),
								SystemColumnImpl.getColumn("JOBCOUNT",Types.INTEGER,true),
								SystemColumnImpl.getColumn("TASKCOUNT",Types.INTEGER,true),
								SystemColumnImpl.getColumn("FAILEDTASKCOUNT",Types.INTEGER,true),
				};
		}

		public static void main(String... args) throws Exception{
				System.out.println(new XPLAINOperationHistoryDescriptor().getTableDDL("APP"));
		}
}
