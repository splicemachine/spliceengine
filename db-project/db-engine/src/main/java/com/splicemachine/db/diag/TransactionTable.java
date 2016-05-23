/*

   Derby - Class com.splicemachine.db.diag.TransactionTable

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.db.diag;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;
import com.splicemachine.db.iapi.store.access.TransactionInfo;

import com.splicemachine.db.vti.VTITemplate;
import com.splicemachine.db.vti.VTICosting;
import com.splicemachine.db.vti.VTIEnvironment;

import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.impl.jdbc.EmbedResultSetMetaData;

import com.splicemachine.db.iapi.reference.Limits;
import com.splicemachine.db.iapi.util.StringUtil;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

/**
	

	TransactionTable is a virtual table that shows all transactions
	currently in the database.
	
	This virtual table can be invoked by calling it
	directly
	<PRE> select * from SYSCS_DIAG.TRANSACTION_TABLE </PRE>

	<P>The TransactionTable virtual table takes a snap shot of the 
	transaction table while the system is in flux, so it is possible that some
	transactions may be in transition state while the snap shot is taken.
	We choose to do this rather then impose extraneous timing restrictions so
	that the use of this tool will not alter the normal timing and flow of
	execution in the application. 

	<P>The TransactionTable virtual table has the following columns:
	<UL>
	<LI>XID varchar(15) - not nullable.  The transaction id, this can be joined
	with the LockTable virtual table's XID.</LI>
	<LI>GLOBAL_XID varchar(140) - nullable.  The global transaction id, only
	set if this transaction is a participant in a distributed transaction.</LI>
	<LI>USERNAME varchar(128) - nullable.  The user name, or SPLICE by default.
	May appear null if the transaction is started by Derby.</LI>
	<LI>TYPE varchar(30) - not nullable. UserTransaction or an internal
	transaction spawned by Derby.</LI>
	<LI>STATUS varchar(8) - not nullable.  IDLE or ACTIVE.  A transaction is
	IDLE only when it is first created or right after it commits.  Any
	transaction that holds or has held any resource in the database is ACTIVE.
	Accessing the TransactionTable virtual table without using the class alias
	will not activate the transaction.</LI>
	<LI>FIRST_INSTANT varchar(20) - nullable.  If null, this is a read only
	transaction.  If not null, this is the first log record instant written by
	the transaction.</LI> 
	<LI>SQL_TEXT VARCHAR(32672) - nullable.  if null, this transaction is
	currently not being executed in the database.  If not null, this is the SQL
	statement currently being executed in the database.</LI>
	</UL>
*/
public class TransactionTable extends VTITemplate implements VTICosting {

	private TransactionInfo[] transactionTable;
	boolean initialized;
	int currentRow;
	private boolean wasNull;

    public  TransactionTable()    throws StandardException
    {
        DiagUtil.checkAccess();
    }
    
	/**
		@see java.sql.ResultSet#getMetaData
	 */
	public ResultSetMetaData getMetaData()
	{
		return metadata;
	}

	/**
		@see java.sql.ResultSet#next
		@exception SQLException if no transaction context can be found
	 */
	public boolean next() throws SQLException
	{
		if (!initialized)
		{
			LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();

			transactionTable = lcc.getTransactionExecute().
			   getAccessManager().getTransactionInfo();

			initialized = true;
			currentRow = -1;
		}

		if (transactionTable == null)
			return false;

		for (currentRow++;
			 currentRow < transactionTable.length;
			 currentRow++)
		{
			TransactionInfo info = transactionTable[currentRow];
			if (info == null)
				continue;		// transaction object in flux while the
								// snap shot was taken, get another row
			return true;
		}

		// currentRow >= transactionTable.length
		transactionTable = null;
		return false;
	}

	/**
		@see java.sql.ResultSet#close
	 */
	public void close()
	{
		transactionTable = null;
	}

	/**
		All columns in TransactionTable VTI is of String type.
		@see java.sql.ResultSet#getString
	 */
	public String getString(int columnNumber)
	{
		TransactionInfo info = transactionTable[currentRow];
		String str = null;

		switch(columnNumber)
		{
		case 1:
			str = info.getTransactionIdString(); break;

		case 2:
			str = info.getGlobalTransactionIdString(); break;

		case 3:
			str = info.getUsernameString(); break;

		case 4:
			str = info.getTransactionTypeString(); break;

		case 5:
			str = info.getTransactionStatusString(); break;

		case 6:
			str = info.getFirstLogInstantString(); break;

		case 7:

			str = info.getStatementTextString();
			str = StringUtil.truncate(str, Limits.DB2_VARCHAR_MAXWIDTH);
			break;

		default:
			str = null;
		}

		wasNull = (str == null);
		return str;

	}

	/**
		@see java.sql.ResultSet#wasNull
	 */
	public boolean wasNull()
	{
		return wasNull;
	}


	/**  VTI costing interface */
	
	/**
		@see VTICosting#getEstimatedRowCount
	 */
	public double getEstimatedRowCount(VTIEnvironment vtiEnvironment)
	{
		return VTICosting.defaultEstimatedRowCount;
	}
	
	/**
		@see VTICosting#getEstimatedCostPerInstantiation
	 */
	public double getEstimatedCostPerInstantiation(VTIEnvironment vtiEnvironment)
	{
		return VTICosting.defaultEstimatedCost;
	}

	/**
		@return false
		@see VTICosting#supportsMultipleInstantiations
	 */
	public boolean supportsMultipleInstantiations(VTIEnvironment vtiEnvironment)
	{
		return false;
	}


	/*
	** Metadata
	*/
	private static final ResultColumnDescriptor[] columnInfo = {

		EmbedResultSetMetaData.getResultColumnDescriptor("XID",           Types.VARCHAR, false, 15),
		EmbedResultSetMetaData.getResultColumnDescriptor("GLOBAL_XID",    Types.VARCHAR, true,  140),
		EmbedResultSetMetaData.getResultColumnDescriptor("USERNAME",      Types.VARCHAR, true,  128),
		EmbedResultSetMetaData.getResultColumnDescriptor("TYPE",          Types.VARCHAR, false, 30),
		EmbedResultSetMetaData.getResultColumnDescriptor("STATUS",        Types.VARCHAR, false, 8),
		EmbedResultSetMetaData.getResultColumnDescriptor("FIRST_INSTANT", Types.VARCHAR, true,  20),
		EmbedResultSetMetaData.getResultColumnDescriptor("SQL_TEXT",      Types.VARCHAR, true,  Limits.DB2_VARCHAR_MAXWIDTH),
	};
	
	private static final ResultSetMetaData metadata = new EmbedResultSetMetaData(columnInfo);
}

