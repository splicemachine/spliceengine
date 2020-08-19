/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.actions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

import com.splicemachine.db.catalog.IndexDescriptor;
import com.splicemachine.db.catalog.types.IndexDescriptorImpl;
import com.splicemachine.db.iapi.error.ExceptionSeverity;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.jdbc.ConnectionContext;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.StatementType;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDescriptorGenerator;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.db.impl.sql.execute.DDLConstantAction;
import com.splicemachine.db.shared.common.reference.SQLState;

/**
 * A Constant Action for creating a table in a Splice-efficient fashion
 *
 * @author Scott Fines
 * Created on: 3/5/13
 */
public class SpliceCreateTableOperation extends CreateTableConstantOperation {
	private final int tableType;
	private final String tableName;
	private final String schemaName;
	private final String withDataQueryString;
	private final int    createBehavior;

	/**
	 * Make the ConstantAction for a CREATE TABLE statement.
	 *
	 * @param schemaName           name for the schema that table lives in.
	 * @param tableName            Name of table.
	 * @param tableType            Type of table (e.g., BASE, global temporary table).
	 * @param columnInfo           Information on all the columns in the table.
	 *                             (REMIND tableDescriptor ignored)
	 * @param constraintActions    CreateConstraintConstantAction[] for constraints
	 * @param properties           Optional table properties
	 * @param createBehavior       CREATE_IF_NOT_EXISTS or CREATE_DEFAULT
	 * @param lockGranularity      The lock granularity.
	 * @param onCommitDeleteRows   If true, on commit delete rows else on commit preserve rows of temporary table.
	 * @param onRollbackDeleteRows If true, on rollback, delete rows from temp tables which were logically modified.
	 *                             true is the only supported value
	 * @param withDataQueryString  this is non-null only when the create table statement contains a query to create
	 *                             and populate the table with a query.
	 */
	public SpliceCreateTableOperation(String schemaName,
									  String tableName,
									  int tableType,
									  ColumnInfo[] columnInfo,
									  ConstantAction[] constraintActions,
									  Properties properties,
									  int createBehavior,
									  char lockGranularity,
									  boolean onCommitDeleteRows,
									  boolean onRollbackDeleteRows,
									  String withDataQueryString,
									  boolean isExternal,
									  String delimited,
									  String escaped,
									  String lines,
									  String storedAs,
									  String location,
									  String compression,
									  boolean mergeSchema,
									  boolean presplit,
									  boolean isLogicalKey,
									  String splitKeyPath,
									  String columnDelimiter,
									  String characterDelimiter,
									  String timestampFormat,
									  String dateFormat,
									  String timeFormat) {
		super(schemaName, tableName, tableType, columnInfo, constraintActions, properties, lockGranularity, onCommitDeleteRows, onRollbackDeleteRows, isExternal,
				delimited, escaped, lines, storedAs, location, compression, mergeSchema,presplit,isLogicalKey,splitKeyPath,
                columnDelimiter,characterDelimiter,timestampFormat,dateFormat,timeFormat);
		this.tableType = tableType;
		this.tableName = tableName;
		this.schemaName = schemaName;
		this.withDataQueryString = withDataQueryString;
		this.createBehavior = createBehavior;
	}

	@Override
	public void executeConstantAction(Activation activation) throws StandardException {
        /*
         * what follows here is a pretty nasty hack to prevent creating duplicate Table entries.
         *
         * There are two underlying problems preventing a more elegant solution here:
         *
         * 1. Derby manages system indices inside of the DataDictionary, rather than
         * allowing transparent insertions. This prevents the Index management coprocessors
         * from managing indices themselves.
         *
         * 2. Derby generates a UUID for a table which it uses as it's key for inserting
         * into its indices (because it manages them itself). Then, the HBase row key for
         * the system indices is just a random Row Key, rather than a primary key-type structure.
         * This means that a UniqueConstraint running in the Index coprocessor will never fail, which
         * will allow duplicates in.
         *
         * The most effective implementation is probably to remove explicit index management
         * from the DataDictionary, which should mean that UniqueConstraints can be enforced like
         * any other table, but that's a huge amount of work for very little reward in this case.
         *
         * Thus, this little hack: Basically, before creating the table, we first scan the SYSTABLES table
         * to see if any table with that name already exists in the specified Schema. If it doesn't, we
         * allow the table to be created; if it does, then we bomb out with an error.
         *
         * The downside? This is a check-then-act operation, which probably isn't thread-safe (and certainly
         * isn't cluster-safe), so we have to be certain that DDL operations occur within a transaction. Even then,
         * we're probably not safe, and this breaks adding tables concurrently. However, I am pretty sure that
         * the DataDictionary in general is not cluster-safe, so we are probably not any more screwed now
         * than we were before. Still, if this causes concurrent table-creations to fail, blame me.
         *
         * -Scott Fines, March 26 2013 -
         */
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		TransactionController tc = lcc.getTransactionExecute();

		SchemaDescriptor sd;
		if(tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)
			sd = dd.getSchemaDescriptor(schemaName,tc, true);
		else
			sd = DDLConstantAction.getSchemaDescriptorForCreate(dd, activation, schemaName);

		TableDescriptor tableDescriptor = dd.getTableDescriptor(tableName, sd,tc);
		if (tableDescriptor != null) {
			StandardException e = StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,
					"table", tableName, "schema", schemaName);
			if (createBehavior == StatementType.CREATE_IF_NOT_EXISTS) {
				e.setSeverity(ExceptionSeverity.WARNING_SEVERITY);
			}
			throw e;
		}

        /*
         * The table didn't exist in a manner which is visible to us, so
         * it should be safe to try and create it.
         *
         */
		//if the table doesn't exist, allow super class to create it
		super.executeConstantAction(activation);

		// If "WITH DATA", parse, plan, etc, the insert query string now that we've created the table
		if (withDataQueryString != null) {
			ConnectionContext cc = (ConnectionContext)
					lcc.getContextManager().getContext(ConnectionContext.CONTEXT_ID);
			activation.setupSQLSessionContextForChildren(false);
			try ( // try with resources
				  Connection conn = cc.getNestedConnection(true);
				  PreparedStatement ps =
						  conn.prepareStatement("insert into \""+schemaName+"\".\""+tableName+"\" "+withDataQueryString)
			)  {
				int rows = ps.executeUpdate();
				activation.addRowsSeen(rows);
			} catch (SQLException e) {
				// TODO: JC - externalize msg
				throw StandardException.newException("Failed execute create table with data insert string.", e);
			}
		}
	}

	@Override
	protected ConglomerateDescriptor getTableConglomerateDescriptor(TableDescriptor td, long conglomId, SchemaDescriptor sd, DataDescriptorGenerator ddg) throws StandardException {
        /*
         * If there is a PrimaryKey Constraint, build an IndexRowGenerator that returns the Primary Keys,
         * otherwise, do whatever Derby does by default
         */
		if(constraintActions ==null)
			return super.getTableConglomerateDescriptor(td,conglomId,sd,ddg);

		for(ConstraintConstantOperation constantAction:constraintActions){
			if(constantAction.getConstraintType()== DataDictionary.PRIMARYKEY_CONSTRAINT){
				int [] pkColumns = ((CreateConstraintConstantOperation)constantAction).genColumnPositions(td, true);
				boolean[] ascending = new boolean[pkColumns.length];
				for(int i=0;i<ascending.length;i++){
					ascending[i] = true;
				}

				IndexDescriptor descriptor = new IndexDescriptorImpl("PRIMARYKEY",true,false,pkColumns,ascending,pkColumns.length,false,false);
				IndexRowGenerator irg = new IndexRowGenerator(descriptor);
				return ddg.newConglomerateDescriptor(conglomId,null,false,irg,false,null,td.getUUID(),sd.getUUID());
			}
		}
		return super.getTableConglomerateDescriptor(td,conglomId,sd,ddg);
	}

	public String getScopeName() {
		return String.format("Create Table %s", tableName);
	}
}
