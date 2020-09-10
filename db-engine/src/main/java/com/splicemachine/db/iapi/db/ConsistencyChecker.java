/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.db;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.error.PublicAPI;

import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;

import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;


import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;

import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.RowUtil;

import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;

import java.sql.SQLException;

/**
 * The ConsistencyChecker class provides static methods for verifying
 * the consistency of the data stored within a database.
 * 
 *
   <p>This class can only be used within an SQL-J statement, a Java procedure or a server side Java method.
   <p>This class can be accessed using the class alias <code> CONSISTENCYCHECKER </code> in SQL-J statements.
 */
public class ConsistencyChecker
{

	/** no requirement for a constructor */
	private ConsistencyChecker() {
	}

	/**
	 * Check the named table, ensuring that all of its indexes are consistent
	 * with the base table.
	 * Use this
	 *  method only within an SQL-J statement; do not call it directly.
	 * <P>When tables are consistent, the method returns true. Otherwise, the method throws an exception.
	 * <p>To check the consistency of a single table:
	 * <p><code>
	 * VALUES ConsistencyChecker::checkTable(<i>SchemaName</i>, <i>TableName</i>)</code></p>
	 * <P>For example, to check the consistency of the table <i>SPLICE.Flights</i>:
	 * <p><code>
	 * VALUES ConsistencyChecker::checkTable('SPLICE', 'FLIGHTS')</code></p>
	 * <p>To check the consistency of all of the tables in the 'SPLICE' schema,
	 * stopping at the first failure: 
	 *
	 * <P><code>SELECT tablename, ConsistencyChecker::checkTable(<br>
	 * 'SPLICE', tablename)<br>
	 * FROM sys.sysschemas s, sys.systables t
	 * WHERE s.schemaname = 'SPLICE' AND s.schemaid = t.schemaid</code>
	 *
	 * <p> To check the consistency of an entire database, stopping at the first failure:
	 *
	 * <p><code>SELECT schemaname, tablename,<br>
	 * ConsistencyChecker::checkTable(schemaname, tablename)<br>
	 * FROM sys.sysschemas s, sys.systables t<br>
	 * WHERE s.schemaid = t.schemaid</code>
	 *
	 *
	 *
	 * @param schemaName	The schema name of the table.
	 * @param tableName		The name of the table
	 *
	 * @return	true, if the table is consistent, exception thrown if inconsistent
	 *
	 * @exception	SQLException	Thrown if some inconsistency
	 *									is found, or if some unexpected
	 *									exception is thrown..
	 */
	public static boolean checkTable(String schemaName, String tableName)
						throws SQLException
	{
		DataDictionary			dd;
		TableDescriptor			td;
		long					baseRowCount = -1;
		TransactionController	tc;
		ConglomerateDescriptor	heapCD;
		ConglomerateDescriptor	indexCD;
		ExecRow					baseRow;
		ExecRow					indexRow;
		RowLocation				rl = null;
		RowLocation				scanRL = null;
		ScanController			scan = null;
		int[]					baseColumnPositions;
		int						baseColumns = 0;
		DataValueFactory		dvf;
		long					indexRows;
		ConglomerateController	baseCC = null;
		ConglomerateController	indexCC = null;
		SchemaDescriptor		sd;
		ConstraintDescriptor	constraintDesc;

		LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
		tc = lcc.getTransactionExecute();

		try {

            dd = lcc.getDataDictionary();

            ExecutionFactory ef = lcc.getLanguageConnectionFactory().getExecutionFactory();

            sd = dd.getSchemaDescriptor(schemaName, tc, true);
            td = dd.getTableDescriptor(tableName, sd, tc);

            if (td == null)
            {
                throw StandardException.newException(
                    SQLState.LANG_TABLE_NOT_FOUND, 
                    schemaName + "." + tableName);
            }

            /* Skip views */
            if (td.getTableType() == TableDescriptor.VIEW_TYPE)
            {
                return true;
            }

			/* Open the heap for reading */
			baseCC = tc.openConglomerate(
			            td.getHeapConglomerateId(), false, 0, 
				        TransactionController.MODE_TABLE,
					    TransactionController.ISOLATION_SERIALIZABLE);

			/* Check the consistency of the heap */
			baseCC.checkConsistency();

			heapCD = td.getConglomerateDescriptor(td.getHeapConglomerateId());

			/* Get a row template for the base table */
			baseRow = ef.getValueRow(td.getNumberOfColumns());

			/* Fill the row with nulls of the correct type */
			ColumnDescriptorList cdl = td.getColumnDescriptorList();
			int					 cdlSize = cdl.size();

			for (int index = 0; index < cdlSize; index++)
			{
				ColumnDescriptor cd = (ColumnDescriptor) cdl.elementAt(index);
				baseRow.setColumn(cd.getPosition(),
										cd.getType().getNull());
			}

			/* Look at all the indexes on the table */
			ConglomerateDescriptor[] cds = td.getConglomerateDescriptors();
            for (ConglomerateDescriptor cd1 : cds) {
                indexCD = cd1;
                /* Skip the heap */
                if (!indexCD.isIndex())
                    continue;

				/* Check the internal consistency of the index */
                indexCC =
                        tc.openConglomerate(
                                indexCD.getConglomerateNumber(),
                                false,
                                0,
                                TransactionController.MODE_TABLE,
                                TransactionController.ISOLATION_SERIALIZABLE);

                indexCC.checkConsistency();
                indexCC.close();
                indexCC = null;

				/* if index is for a constraint check that the constraint exists */

                if (indexCD.isConstraint()) {
                    constraintDesc = dd.getConstraintDescriptor(td, indexCD.getUUID());
                    if (constraintDesc == null) {
                        throw StandardException.newException(
                                SQLState.LANG_OBJECT_NOT_FOUND,
                                "CONSTRAINT for INDEX",
                                indexCD.getConglomerateName());
                    }
                }

				/*
				** Set the base row count when we get to the first index.
				** We do this here, rather than outside the index loop, so
				** we won't do the work of counting the rows in the base table
				** if there are no indexes to check.
				*/
                if (baseRowCount < 0) {
                    scan = tc.openScan(heapCD.getConglomerateNumber(),
                            false,    // hold
                            0,        // not forUpdate
                            TransactionController.MODE_TABLE,
                            TransactionController.ISOLATION_SERIALIZABLE,
                            RowUtil.EMPTY_ROW_BITSET,
                            null,    // startKeyValue
                            0,        // not used with null start posn.
                            null,    // qualifier
                            null,    // stopKeyValue
                            0);        // not used with null stop posn.

					/* Also, get the row location template for index rows */
                    rl = scan.newRowLocationTemplate();
                    scanRL = scan.newRowLocationTemplate();

                    for (baseRowCount = 0; scan.next(); baseRowCount++)
                        ;	/* Empty statement */

                    scan.close();
                    scan = null;
                }

                baseColumnPositions =
                        indexCD.getIndexDescriptor().baseColumnPositions();
                baseColumns = baseColumnPositions.length;

                FormatableBitSet indexColsBitSet = new FormatableBitSet();
                for (int i = 0; i < baseColumns; i++) {
                    indexColsBitSet.grow(baseColumnPositions[i]);
                    indexColsBitSet.set(baseColumnPositions[i] - 1);
                }

				/* Get one row template for the index scan, and one for the fetch */
                indexRow = ef.getValueRow(baseColumns + 1);

				/* Fill the row with nulls of the correct type */
                for (int column = 0; column < baseColumns; column++) {
					/* Column positions in the data dictionary are one-based */
                    ColumnDescriptor cd = td.getColumnDescriptor(baseColumnPositions[column]);
                    indexRow.setColumn(column + 1,
                            cd.getType().getNull());
                }

				/* Set the row location in the last column of the index row */
                indexRow.setColumn(baseColumns + 1, rl);

				/* Do a full scan of the index */
                scan = tc.openScan(indexCD.getConglomerateNumber(),
                        false,    // hold
                        0,        // not forUpdate
                        TransactionController.MODE_TABLE,
                        TransactionController.ISOLATION_SERIALIZABLE,
                        (FormatableBitSet) null,
                        null,    // startKeyValue
                        0,        // not used with null start posn.
                        null,    // qualifier
                        null,    // stopKeyValue
                        0);        // not used with null stop posn.

                DataValueDescriptor[] baseRowIndexOrder =
                        new DataValueDescriptor[baseColumns];
                DataValueDescriptor[] baseObjectArray = baseRow.getRowArray();

                for (int i = 0; i < baseColumns; i++) {
                    baseRowIndexOrder[i] = baseObjectArray[baseColumnPositions[i] - 1];
                }
			
				/* Get the index rows and count them */
                for (indexRows = 0; scan.fetchNext(indexRow.getRowArray()); indexRows++) {
					/*
					** Get the base row using the RowLocation in the index row,
					** which is in the last column.  
					*/
                    RowLocation baseRL = (RowLocation) indexRow.getColumn(baseColumns + 1);
                    ExecRow row = new ValueRow();
                    row.setRowArray(baseObjectArray);
                    boolean base_row_exists =
                            baseCC.fetch(
                                    baseRL, row, indexColsBitSet);

					/* Throw exception if fetch() returns false */
                    if (!base_row_exists) {
                        String indexName = indexCD.getConglomerateName();
                        throw StandardException.newException(SQLState.LANG_INCONSISTENT_ROW_LOCATION,
                                (schemaName + "." + tableName),
                                indexName,
                                baseRL.toString(),
                                indexRow.toString());
                    }

					/* Compare all the column values */
                    for (int column = 0; column < baseColumns; column++) {
                        DataValueDescriptor indexColumn =
                                indexRow.getColumn(column + 1);
                        DataValueDescriptor baseColumn =
                                baseRowIndexOrder[column];

						/*
						** With this form of compare(), null is considered equal
						** to null.
						*/
                        if (indexColumn.compare(baseColumn) != 0) {
                            ColumnDescriptor cd =
                                    td.getColumnDescriptor(
                                            baseColumnPositions[column]);

                            /*
                            System.out.println(
                                "SQLState.LANG_INDEX_COLUMN_NOT_EQUAL:" +
                                "indexCD.getConglomerateName()" + indexCD.getConglomerateName() +
                                ";td.getSchemaName() = " + td.getSchemaName() +
                                ";td.getName() = " + td.getName() +
                                ";baseRL.toString() = " + baseRL.toString() +
                                ";cd.getColumnName() = " + cd.getColumnName() +
                                ";indexColumn.toString() = " + indexColumn.toString() +
                                ";baseColumn.toString() = " + baseColumn.toString() +
                                ";indexRow.toString() = " + indexRow.toString());
                            */

                            throw StandardException.newException(
                                    SQLState.LANG_INDEX_COLUMN_NOT_EQUAL,
                                    indexCD.getConglomerateName(),
                                    td.getSchemaName(),
                                    td.getName(),
                                    baseRL.toString(),
                                    cd.getColumnName(),
                                    indexColumn.toString(),
                                    baseColumn.toString(),
                                    indexRow.toString());
                        }
                    }
                }

				/* Clean up after the index scan */
                scan.close();
                scan = null;

				/*
				** The index is supposed to have the same number of rows as the
				** base conglomerate.
				*/
                if (indexRows != baseRowCount) {
                    throw StandardException.newException(SQLState.LANG_INDEX_ROW_COUNT_MISMATCH,
                            indexCD.getConglomerateName(),
                            td.getSchemaName(),
                            td.getName(),
                            Long.toString(indexRows),
                            Long.toString(baseRowCount));
                }
            }
			/* check that all constraints have backing index */
			ConstraintDescriptorList constraintDescList = 
				dd.getConstraintDescriptors(td);
			for (int index = 0; index < constraintDescList.size(); index++)
			{
				constraintDesc = constraintDescList.elementAt(index);
				if (constraintDesc.hasBackingIndex())
				{
					ConglomerateDescriptor conglomDesc;

					conglomDesc = td.getConglomerateDescriptor(
							constraintDesc.getConglomerateId());
					if (conglomDesc == null)
					{
						throw StandardException.newException(
										SQLState.LANG_OBJECT_NOT_FOUND,
										"INDEX for CONSTRAINT",
										constraintDesc.getConstraintName());
					}
				}
			}
			
		}
		catch (StandardException se)
		{
			throw PublicAPI.wrapStandardException(se);
		}
		finally
		{
            try
            {
                /* Clean up before we leave */
                if (baseCC != null)
                {
                    baseCC.close();
                    baseCC = null;
                }
                if (indexCC != null)
                {
                    indexCC.close();
                    indexCC = null;
                }
                if (scan != null)
                {
                    scan.close();
                    scan = null;
                }
            }
            catch (StandardException se)
            {
                throw PublicAPI.wrapStandardException(se);
            }
		}

		return true;
	}
}
