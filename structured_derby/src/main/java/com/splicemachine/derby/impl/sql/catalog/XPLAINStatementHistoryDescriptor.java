package com.splicemachine.derby.impl.sql.catalog;

import org.apache.derby.iapi.sql.dictionary.SystemColumn;
import org.apache.derby.impl.sql.catalog.SystemColumnImpl;

import java.sql.Types;

/**
 * @author Scott Fines
 * Date: 1/21/14
 */
public class XPLAINStatementHistoryDescriptor extends XPLAINTaskDescriptor {
		private static final String TABLE_NAME="SYSXPLAIN_STATEMENTHISTORY";

		private static final String STATEMENTID_NAME="STATEMENTID";
		private static final String HOST_NAME="HOST";
		private static final String USER_NAME = "USERNAME";

		@Override
		public String getTableName() {
				return TABLE_NAME;
		}

		@Override
		protected String[] getPrimaryKeys() {
				return new String[]{STATEMENTID_NAME,HOST_NAME,USER_NAME};
		}

		@Override
		protected SystemColumn[] buildColumnList() {
				return new SystemColumn[]{
								SystemColumnImpl.getColumn(STATEMENTID_NAME, Types.BIGINT,false),
								SystemColumnImpl.getColumn(HOST_NAME,Types.VARCHAR,false,32642),
								SystemColumnImpl.getColumn(USER_NAME,Types.VARCHAR,false,32642),
								SystemColumnImpl.getColumn("TRANSACTIONID",Types.BIGINT,false),
								SystemColumnImpl.getColumn("STATUS",Types.VARCHAR,false,10),
								SystemColumnImpl.getColumn("STATEMENTSQL",Types.VARCHAR,false,32642),
								SystemColumnImpl.getColumn("TOTALJOBCOUNT",Types.INTEGER,true),
								SystemColumnImpl.getColumn("SUCCESSFULJOBS",Types.INTEGER,true),
								SystemColumnImpl.getColumn("FAILEDJOBS",Types.INTEGER,true),
								SystemColumnImpl.getColumn("CANCELLEDJOBS",Types.INTEGER,true),
								SystemColumnImpl.getColumn("STARTTIMEMS",Types.BIGINT,true),
								SystemColumnImpl.getColumn("STOPTIMEMS",Types.BIGINT,true),
				};
		}
		public static void main(String... args) throws Exception{
				System.out.println(new XPLAINStatementHistoryDescriptor().getTableDDL("APP"));
		}
}
