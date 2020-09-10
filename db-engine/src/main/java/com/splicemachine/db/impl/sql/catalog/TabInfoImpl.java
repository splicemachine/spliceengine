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

package com.splicemachine.db.impl.sql.catalog;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.StreamStorable;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.dictionary.CatalogRowFactory;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.RowChanger;
import com.splicemachine.db.iapi.sql.execute.TupleFilter;
import com.splicemachine.db.iapi.store.access.*;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

/**
 * A poor mans structure used in DataDictionaryImpl.java.
 * Used to save heapId, name pairs for non core tables.
 *
 */
public class TabInfoImpl
{
    /**
     * ROWNOTDUPLICATE is out of range for a row
     * number.  If a return code does not equal
     * this value, then it refers to the row
     * that is a duplicate.
     */
    public static final   int     ROWNOTDUPLICATE = -1;

    private IndexInfoImpl[]				indexes;
    private long						heapConglomerate;
    private int							numIndexesSet;
    private boolean						heapSet;
    private final CatalogRowFactory			crf;

    /**
     * Constructor
     *
     * @param crf				the associated CatalogRowFactory
     */
    public TabInfoImpl(CatalogRowFactory crf)
    {
        this.heapConglomerate = -1;
        this.crf = crf;

        int numIndexes = crf.getNumIndexes();

        if (numIndexes > 0)
        {
            indexes = new IndexInfoImpl[numIndexes];

			/* Init indexes */
            for (int indexCtr = 0; indexCtr < numIndexes; indexCtr++)
            {
                indexes[indexCtr] = new IndexInfoImpl(
                        indexCtr,
                        crf);
            }
        }
    }

    /**
     * Get the conglomerate for the heap.
     *
     * @return long     The conglomerate for the heap.
     */
    public long getHeapConglomerate()
    {
        return heapConglomerate;
    }

    /**
     * Set the heap conglomerate for this.
     *
     * @param heapConglomerate  The new heap conglomerate.
     */
    public void setHeapConglomerate(long heapConglomerate)
    {
        this.heapConglomerate = heapConglomerate;
        heapSet = true;
        crf.setHeapConglomerate(heapConglomerate);
    }

    /**
     * Get the conglomerate for the specified index.
     *
     * @return long     The conglomerate for the specified index.
     */
    public long getIndexConglomerate(int indexID)
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(indexes != null,
                    "indexes is expected to be non-null");
            if (indexID >= indexes.length)
            {
                SanityManager.THROWASSERT(
                        "indexID (" + indexID + ") is out of range(0-" +
                                indexes.length + ")");
            }
        }

        return indexes[indexID].getConglomerateNumber();
    }

    /**
     * Set the index conglomerate for the table.
     *
     * @param index             Index number for index for table
     * @param indexConglomerate The conglomerate for that index
     */
    void setIndexConglomerate(int index, long indexConglomerate)
    {
		/* Index names must be set before conglomerates.
		 * Also verify that we are not setting the same conglomerate
		 * twice.
		 */
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(indexes[index] != null,
                    "indexes[index] expected to be non-null");
            SanityManager.ASSERT(indexes[index].getConglomerateNumber() == -1,
                    "indexes[index] expected to be -1");
        }
        indexes[index].setConglomerateNumber(indexConglomerate);

		/* We are completely initialized when all indexes have 
		 * their conglomerates initialized 
		 */
        numIndexesSet++;
    }

    /**
     * Set the index conglomerate for the table.
     *
     * @param cd    The ConglomerateDescriptor for one of the index
     *              for this table.
     */
    void setIndexConglomerate(ConglomerateDescriptor cd)
    {
        int		index;
        String	indexName = cd.getConglomerateName();

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(indexes != null,
                    "indexes is expected to be non-null");
        }

        for (index = 0; index < indexes.length; index++)
        {
			/* All index names expected to be set before
			 * any conglomerate is set.
			 */
            if (SanityManager.DEBUG)
            {
                SanityManager.ASSERT(indexes[index] != null,
                        "indexes[index] expected to be non-null");
                SanityManager.ASSERT(indexes[index].getIndexName() != null,
                        "indexes[index].getIndexName() expected to be non-null");
            }

			/* Do we have a match? */
            if (indexes[index].getIndexName().equals(indexName))
            {
                indexes[index].setConglomerateNumber(cd.getConglomerateNumber());
                break;
            }
        }

        if (SanityManager.DEBUG)
        {
            if (index == indexes.length)
            {
                SanityManager.THROWASSERT("match not found for " + indexName);
            }
        }

		/* We are completely initialized when all indexIds are initialized */
        numIndexesSet++;
    }

    /**
     * Get the table name.
     *
     * @return String   The table name.
     */
    public String getTableName()
    {
        return crf.getCatalogName();
    }

    /**
     * Get the index name.
     *
     * @param indexId   Index number for index for table
     *
     * @return String   The index name.
     */
    String getIndexName(int indexId)
    {
        return indexes[indexId].getIndexName();
    }

    /**
     * Get the CatalogRowFactory for this.
     *
     * @return CatalogRowFactory    The CatalogRowFactory for this.
     */
    public CatalogRowFactory getCatalogRowFactory()
    {
        return crf;
    }

    /**
     * Is this fully initialized.  
     * (i.e., is all conglomerate info initialized)
     *
     * @return boolean  Whether or not this is fully initialized.
     */
    boolean isComplete() {
        /* We are complete when heap conglomerate and all
		 * index conglomerates are set.
		 */
        return heapSet && (indexes == null || indexes.length == numIndexesSet);
    }

    /**
     * Get the column count for the specified index number.
     *
     * @param indexNumber   The index number.
     *
     * @return int          The column count for the specified index.
     */
    int getIndexColumnCount(int indexNumber)
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(indexes != null,
                    "indexes is expected to be non-null");

            if (!(indexNumber < indexes.length))
            {
                SanityManager.THROWASSERT("indexNumber (" + indexNumber + ") is out of range(0-" +
                        indexes.length + ")");
            }
        }

        return indexes[indexNumber].getColumnCount();
    }

    /**
     * Get the IndexRowGenerator for the specified index number.
     *
     * @param indexNumber   The index number.
     *
     * @return IndexRowGenerator    The IRG for the specified index number.
     */
    public IndexRowGenerator getIndexRowGenerator(int indexNumber)
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(indexes != null,
                    "indexes is expected to be non-null");
            if (indexNumber >= indexes.length)
            {
                SanityManager.THROWASSERT(
                        "indexNumber (" + indexNumber + ") is out of range(0-" +
                                indexes.length + ")");
            }
        }
        return indexes[indexNumber].getIndexRowGenerator();
    }

    /**
     * Set the IndexRowGenerator for the specified index number.
     *
     * @param indexNumber   The index number.
     * @param irg           The IndexRowGenerator for the specified index number.
     */
    void setIndexRowGenerator(int indexNumber, IndexRowGenerator irg)
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(indexes != null,
                    "indexes is expected to be non-null");
            if (indexNumber >= indexes.length)
            {
                SanityManager.THROWASSERT(
                        "indexNumber (" + indexNumber + ") is out of range(0-" +
                                indexes.length + ")");
            }
        }

        indexes[indexNumber].setIndexRowGenerator(irg);
    }

    /**
     * Get the number of indexes on this catalog.
     *
     * @return int  The number of indexes on this catalog.
     */
    public int getNumberOfIndexes()
    {
        if (indexes == null)
        {
            return 0;
        }
        else
        {
            return indexes.length;
        }
    }

    /**
     * Get the base column position for a column within a catalog
     * given the (0-based) index number for this catalog and the
     * (0-based) column number for the column within the index.
     *
     * @param indexNumber   The index number
     * @param colNumber     The column number within the index
     *
     * @return int      The base column position for the column.
     */
    int getBaseColumnPosition(int indexNumber, int colNumber)
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(indexes != null,
                    "indexes is expected to be non-null");
            if (indexNumber >= indexes.length)
            {
                SanityManager.THROWASSERT("indexNumber (" + indexNumber + ") is out of range(0-" +
                        indexes.length + ")");
            }
        }

        return indexes[indexNumber].getBaseColumnPosition(colNumber);
    }

    /**
     * Return whether or not this index is declared unique
     *
     * @param indexNumber   The index number
     *
     * @return boolean      Whether or not this index is declared unique
     */
    boolean isIndexUnique(int indexNumber)
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(indexes != null,
                    "indexes is expected to be non-null");

            if (indexNumber >= indexes.length)
            {
                SanityManager.THROWASSERT("indexNumber (" + indexNumber + ") is out of range(0-" +
                        indexes.length + ")");
            }
        }

        return indexes[indexNumber].isIndexUnique();
    }

    /**
     * Inserts a base row into a catalog and inserts all the corresponding
     * index rows.
     *
     *	@param	row			row to insert
     *	@param	tc			transaction
     *	@return	row number (>= 0) if duplicate row inserted into an index
     *			ROWNOTDUPLICATE otherwise
     *
     * @exception StandardException		Thrown on failure
     */
    public int insertRow( ExecRow row, TransactionController tc)
            throws StandardException
    {

        RowLocation[] 			notUsed = new RowLocation[1];

        return insertRowListImpl(new ExecRow[] {row},tc,notUsed);
    }

    /**
     * Inserts a list of base rows into a catalog and inserts all the corresponding
     * index rows.
     *
     *	@param	rowList		List of rows to insert
     *	@param	tc			transaction controller
     *
     *
     *	@return	row  number (>= 0) if duplicate row inserted into an index
     *			ROWNOTDUPLICATE otherwise
     *
     * @exception StandardException		Thrown on failure
     */
    int insertRowList(ExecRow[] rowList, TransactionController tc )
            throws StandardException
    {
        RowLocation[] 			notUsed = new RowLocation[1];

        return insertRowListImpl(rowList,tc,notUsed);
    }

    /**
     Insert logic to insert a list of rows into a table. This logic has two
     odd features.

     <OL>
     <LI>Returns an indication if any returned row was a duplicate.
     <LI>Returns the RowLocation of the last row inserted.
     </OL>
     @param rowList the list of rows to insert
     @param tc	transaction controller
     @param rowLocationOut on output rowLocationOut[0] is set to the
     last RowLocation inserted.
     @return row number (>= 0) if duplicate row inserted into an index
     ROWNOTDUPLICATE otherwise
     */
    private int insertRowListImpl(ExecRow[] rowList, TransactionController tc,
                                  RowLocation[] rowLocationOut)
            throws StandardException
    {
        ConglomerateController		heapController;
        RowLocation					heapLocation;
        ExecIndexRow				indexableRow;
        int							insertRetCode;
        int							retCode = ROWNOTDUPLICATE;
        int							indexCount = crf.getNumIndexes();
        ConglomerateController[]	indexControllers = new ConglomerateController[ indexCount ];

        // Open the conglomerates
        heapController =
                tc.openConglomerate(
                        getHeapConglomerate(),
                        false,
                        TransactionController.OPENMODE_FORUPDATE,
                        TransactionController.MODE_RECORD,
                        TransactionController.ISOLATION_REPEATABLE_READ);
		
		/* NOTE: Due to the lovely problem of trying to add
		 * a new column to syscolumns and an index on that
		 * column during upgrade, we have to deal with the
		 * issue of the index not existing yet.  So, it's okay
		 * if the index doesn't exist yet.  (It will magically
		 * get created at a later point during upgrade.)
		 */

        for ( int ictr = 0; ictr < indexCount; ictr++ )
        {
            long conglomNumber = getIndexConglomerate(ictr);
            if (conglomNumber > -1)
            {
                indexControllers[ ictr ] =
                        tc.openConglomerate(
                                conglomNumber,
                                false,
                                TransactionController.OPENMODE_FORUPDATE,
                                TransactionController.MODE_RECORD,
                                TransactionController.ISOLATION_REPEATABLE_READ);
            }
        }

        heapLocation = heapController.newRowLocationTemplate();
        rowLocationOut[0]=heapLocation;

        RowLocation[] rowLocations = new RowLocation[rowList.length];
        for ( int i = 0; i< rowLocations.length;i++) {
            rowLocations[i] = heapController.newRowLocationTemplate();
        }

        heapController.batchInsertAndFetchLocation(rowList,rowLocations);
            for ( int ictr = 0; ictr < indexCount; ictr++ ) {
                if (indexControllers[ ictr ] == null) {
                    continue;
                }
                List<ExecRow> indexRows = new ArrayList(rowList.length);
                for (int i =0; i< rowList.length; i++) {
                    // Get an index row based on the base row
                    indexRows.add(getIndexRowFromHeapRow(getIndexRowGenerator(ictr),
                            rowLocations[i],
                            rowList[i]).getClone());
                }
                insertRetCode = indexControllers[ ictr ].batchInsert(indexRows);
                if ( insertRetCode == ConglomerateController.ROWISDUPLICATE ) {
                    retCode = 1;
                }
            }
        // Close the open conglomerates
        for ( int ictr = 0; ictr < indexCount; ictr++ ) {
            if (indexControllers[ ictr ] == null) {
                continue;
            }
            indexControllers[ ictr ].close();
        }
        heapController.close();
        return	retCode;
    }

    public int insertIndexRowListImpl(ExecRow[] rowList,
                                      RowLocation[] rowLocationList,
                                      TransactionController tc,
                                      int ictr, // index number
                                      int numRows)
            throws StandardException
    {
        int							insertRetCode;
        int							retCode = ROWNOTDUPLICATE;
        ConglomerateController	indexController = null;

        // Open the conglomerates
        long conglomNumber = getIndexConglomerate(ictr);
        if (conglomNumber > -1) {
            indexController =
                    tc.openConglomerate(
                            conglomNumber,
                            false,
                            TransactionController.OPENMODE_FORUPDATE,
                            TransactionController.MODE_RECORD,
                            TransactionController.ISOLATION_REPEATABLE_READ);
        }

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(indexController != null,
                        "indexController is expected to be non-null");
        }

        // loop through rows on this list, inserting them into system table
        List<ExecRow> indexRows = new ArrayList(numRows);
        for (int rowNumber = 0; rowNumber < numRows; rowNumber++) {
            // Get an index row based on the base row
            indexRows.add(getIndexRowFromHeapRow(getIndexRowGenerator(ictr),
                    rowLocationList[rowNumber],
                    rowList[rowNumber]).getClone());
        }
        insertRetCode = indexController.batchInsert(indexRows);
        if ( insertRetCode == ConglomerateController.ROWISDUPLICATE ) {
            retCode = 1;
        }

        // Close the open conglomerates
        indexController.close();

        return	retCode;
    }

    /**
     * Given a key row, delete all matching heap rows and their index
     * rows.
     * <p>
     * LOCKING: row locking if there is a key; otherwise,
     * table locking.
     *
     * @param  tc          transaction controller
     * @param  key         key to delete by.
     * @param  indexNumber Key is appropriate for this index.
     * @return the number of rows deleted. If key is not unique,
     *         this may be more than one.
     * @exception StandardException        Thrown on failure
     */
    int deleteRow( TransactionController tc, ExecIndexRow key, int indexNumber, FormatableBitSet cols)
            throws StandardException
    {
        // Always row locking
        return  deleteRows(tc,
                key,
                ScanController.GE,
                null,
                null,
                key,
                ScanController.GT,
                indexNumber,
                true,
                cols);
    }

    int deleteRow( TransactionController tc, ExecIndexRow key, int indexNumber)
            throws StandardException
    {
        // Always row locking
        return  deleteRows(tc,
                key,
                ScanController.GE,
                null,
                null,
                key,
                ScanController.GT,
                indexNumber,
                true,
                null);
    }

    int deleteRow( TransactionController tc, ExecIndexRow key,
                   int indexNumber, boolean wait)
            throws StandardException
    {
        //  Always row locking
        return  deleteRows(tc,
                key,
                ScanController.GE,
                null,
                null,
                key,
                ScanController.GT,
                indexNumber,
                wait,
                null);
    }

    /**
     * Delete the set of rows defined by a scan on an index
     * from the table. Most of the parameters are simply passed
     * to TransactionController.openScan. Please refer to the
     * TransactionController documentation for details.
     * <p>
     * LOCKING: row locking if there is a start and a stop
     * key; otherwise, table locking
     *
     * @param  tc          transaction controller
     * @param  startKey    key to start the scan.
     * @param  startOp     operation to start the scan.
     * @param  stopKey     key to start the scan.
     * @param  qualifier   a qualifier for the scan.
     * @param  filter      filter on base rows
     * @param  stopOp      operation to start the scan.
     * @param  indexNumber Key is appropriate for this index.
     * @return the number of rows deleted.
     * @exception StandardException        Thrown on failure
     * @see TransactionController#openScan
     */
    int deleteRows(TransactionController tc,
                   ExecIndexRow startKey,
                   int startOp,
                   Qualifier[][] qualifier,
                   TupleFilter filter,
                   ExecIndexRow stopKey,
                   int stopOp,
                   int indexNumber) throws StandardException
    {
        return  deleteRows(tc,
                startKey,
                startOp,
                qualifier,
                filter,
                stopKey,
                stopOp,
                indexNumber,
                true,
                null);
    }

    /**
     * @inheritDoc
     */
    private int deleteRows(TransactionController tc,
                           ExecIndexRow startKey,
                           int startOp,
                           Qualifier[][] qualifier,
                           TupleFilter filter,
                           ExecIndexRow stopKey,
                           int stopOp,
                           int indexNumber,
                           boolean wait,
                           FormatableBitSet cols)
            throws StandardException
    {
        ConglomerateController		heapCC;
        ScanController				drivingScan;
        ExecIndexRow	 			drivingIndexRow;
        RowLocation					baseRowLocation;
        RowChanger 					rc;
        ExecRow						baseRow = crf.makeEmptyRow();
        int                         rowsDeleted = 0;
        boolean						passedFilter = true;

        rc = getRowChanger( tc, (int[])null,baseRow );

		/*
		** If we have a start and a stop key, then we are going to 
		** get row locks, otherwise, we are getting table locks.
		** This may be excessive locking for the case where there
		** is a start key and no stop key or vice versa.
		*/
        int lockMode = ((startKey != null) && (stopKey != null)) ?
                TransactionController.MODE_RECORD :
                TransactionController.MODE_TABLE;

		/*
		** Don't use level 3 if we have the same start/stop key.
		*/
        int isolation =
                ((startKey != null) && (stopKey != null) && (startKey == stopKey)) ?
                        TransactionController.ISOLATION_REPEATABLE_READ :
                        TransactionController.ISOLATION_SERIALIZABLE;

        // Row level locking
        rc.open(lockMode, wait);

        DataValueDescriptor[] startKeyRow =
                startKey == null ? null : startKey.getRowArray();

        DataValueDescriptor[] stopKeyRow =
                stopKey == null  ? null : stopKey.getRowArray();

		/* Open the heap conglomerate */
        heapCC = tc.openConglomerate(
                getHeapConglomerate(),
                false,
                (TransactionController.OPENMODE_FORUPDATE |
                        ((wait) ? 0 : TransactionController.OPENMODE_LOCK_NOWAIT)),
                lockMode,
                TransactionController.ISOLATION_REPEATABLE_READ);

        drivingScan = tc.openScan(
                getIndexConglomerate(indexNumber),  // conglomerate to open
                false, // don't hold open across commit
                (TransactionController.OPENMODE_FORUPDATE |
                        ((wait) ? 0 : TransactionController.OPENMODE_LOCK_NOWAIT)),
                lockMode,
                isolation,
                (FormatableBitSet) null, // all fields as objects
                startKeyRow,   // start position - first row
                startOp,      // startSearchOperation
                qualifier, //scanQualifier
                stopKeyRow,   // stop position - through last row
                stopOp);     // stopSearchOperation

        // Get an index row based on the base row
        drivingIndexRow = getIndexRowFromHeapRow(
                getIndexRowGenerator( indexNumber ),
                heapCC.newRowLocationTemplate(),
                crf.makeEmptyRow());

        while (drivingScan.fetchNext(drivingIndexRow.getRowArray()))
        {
            baseRowLocation = (RowLocation)
                    drivingIndexRow.getColumn(drivingIndexRow.nColumns());

            boolean base_row_exists =
                    heapCC.fetch(
                            baseRowLocation, baseRow, (FormatableBitSet) cols);

            if (SanityManager.DEBUG)
            {
                // it can not be possible for heap row to disappear while 
                // holding scan cursor on index at ISOLATION_REPEATABLE_READ.
                SanityManager.ASSERT(base_row_exists, "base row not found");
            }

            // only delete rows which pass the base-row filter
            if ( filter != null ) { passedFilter = filter.execute( baseRow ).equals( true ); }
            if ( passedFilter )
            {
                rc.deleteRow( baseRow, baseRowLocation );
                rowsDeleted++;
            }
        }

        heapCC.close();
        drivingScan.close();
        rc.close();

        // Create a savepoint so that any subsequent writes to the rows we just deleted don't end up
        // with the same timestamp, see DB-9553
        tc.setSavePoint("DD_SAVEPOINT-" + UUID.randomUUID().toString(), null);
        tc.elevate("dictionary");

        return rowsDeleted;
    }

    /**
     * Delete row given its row location
     * @param tc
     * @param rowLocation
     * @param wait
     * @param cols
     * @return
     * @throws StandardException
     */
    public int deleteRowBasedOnRowLocation(TransactionController tc,
                                            RowLocation rowLocation,
                                            boolean wait,
                                            FormatableBitSet cols)
            throws StandardException
    {
        ConglomerateController		heapCC;

        RowChanger 					rc;
        ExecRow						baseRow = crf.makeEmptyRow();
        int                         rowsDeleted = 0;

        rc = getRowChanger( tc, (int[])null,baseRow );

        // we know the row location
        int lockMode = tc.MODE_RECORD;

        // Row level locking
        rc.open(lockMode, wait);

		/* Open the heap conglomerate */
        heapCC = tc.openConglomerate(
                getHeapConglomerate(),
                false,
                (TransactionController.OPENMODE_FORUPDATE |
                        ((wait) ? 0 : TransactionController.OPENMODE_LOCK_NOWAIT)),
                lockMode,
                TransactionController.ISOLATION_REPEATABLE_READ);

        boolean row_exists =
                heapCC.fetch(
                        rowLocation, baseRow, (FormatableBitSet) cols);

        if (!row_exists) {
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(row_exists, "base row not found");
            }
            return 0;
        }
        rc.deleteRow( baseRow, rowLocation);

        heapCC.close();
        rc.close();

        return 1;

    }

    /**
     * Given a key row, return the first matching heap row.
     * <p>
     * LOCKING: shared row locking.
     *
     * @param  tc          transaction controller
     * @param  key         key to read by.
     * @param  indexNumber Key is appropriate for this index.
     * @exception StandardException        Thrown on failure
     */
    ExecRow getRow( TransactionController tc,
                    ExecIndexRow key,
                    int indexNumber )
            throws StandardException
    {
        ConglomerateController		heapCC;

		/* Open the heap conglomerate */
        heapCC = tc.openConglomerate(
                getHeapConglomerate(),
                false,
                0, 						// for read only
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ);

        try { return getRow( tc, heapCC, key, indexNumber ); }
        finally { heapCC.close(); }
    }

    /**
     * Given an index row and index number return the RowLocation
     * in the heap of the first matching row.
     * Used by the autoincrement code to get the RowLocation in
     * syscolumns given a <tablename, columname> pair.
     *
     * @see DataDictionaryImpl#computeRowLocation(TransactionController, TableDescriptor, String)
     *
     * @param tc		  Transaction Controller to use.
     * @param key		  Index Row to search in the index.
     * @param indexNumber Identifies the index to use.
     *
     * @exception		  StandardException thrown on failure.
     */
    RowLocation getRowLocation(TransactionController tc,
                               ExecIndexRow key,
                               int indexNumber)
            throws StandardException
    {
        ConglomerateController		heapCC;
        heapCC = tc.openConglomerate(
                getHeapConglomerate(),
                false,
                0, 						// for read only
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ);

        try
        {
            RowLocation rl[] = new RowLocation[1];
            getRowInternal(tc, heapCC, key, indexNumber, rl);
            return rl[0];
        }
        finally
        {
            heapCC.close();
        }
    }
    /**
     * Given a key row, return the first matching heap row.
     * <p>
     * LOCKING: shared row locking.
     *
     * @param  tc          transaction controller
     * @param  heapCC      heap to look in
     * @param  key         key to read by.
     * @param  indexNumber Key is appropriate for this index.
     * @exception StandardException        Thrown on failure
     */
    ExecRow getRow( TransactionController tc,
                    ConglomerateController heapCC,
                    ExecIndexRow key,
                    int indexNumber)

            throws StandardException
    {
        RowLocation rl[] = new RowLocation[1];
        return getRowInternal(tc, heapCC, key, indexNumber, rl);
    }

    /**
     * @exception StandardException		Thrown on failure
     */
    private ExecRow getRowInternal( TransactionController tc,
                                    ConglomerateController heapCC,
                                    ExecIndexRow key,
                                    int indexNumber,
                                    RowLocation rl[])

            throws StandardException
    {
        ScanController				drivingScan;
        ExecIndexRow	 			drivingIndexRow;
        RowLocation					baseRowLocation;
        ExecRow						baseRow = crf.makeEmptyRow();

        drivingScan = tc.openScan(
                getIndexConglomerate(indexNumber),
                // conglomerate to open
                false,               // don't hold open across commit
                0,                   // open for read
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ,
                (FormatableBitSet) null,      // all fields as objects
                key.getRowArray(),   // start position - first row
                ScanController.GE,   // startSearchOperation
                null,                //scanQualifier
                key.getRowArray(),   // stop position - through last row
                ScanController.GT);  // stopSearchOperation

        // Get an index row based on the base row
        drivingIndexRow = getIndexRowFromHeapRow(
                getIndexRowGenerator( indexNumber ),
                heapCC.newRowLocationTemplate(),
                crf.makeEmptyRow());

        try	{
            if (drivingScan.fetchNext(drivingIndexRow.getRowArray()))
            {
                rl[0] = baseRowLocation = (RowLocation)
                        drivingIndexRow.getColumn(drivingIndexRow.nColumns());
                boolean base_row_exists =
                        heapCC.fetch(
                                baseRowLocation, baseRow, (FormatableBitSet) null);

                if (SanityManager.DEBUG)
                {
                    // it can not be possible for heap row to disappear while 
                    // holding scan cursor on index at ISOLATION_REPEATABLE_READ.
                    SanityManager.ASSERT(base_row_exists, "base row not found");
                }

                return baseRow;
            }
            else
            {
                return null;
            }
        }

        finally {
            drivingScan.close();
        }
    }

    /**
     * Updates a base row in a catalog and updates all the corresponding
     * index rows.
     *
     *	@param	key			key row
     *	@param	newRow		new version of the row
     *	@param	indexNumber	index that key operates
     *	@param	indicesToUpdate	array of booleans, one for each index on the catalog.
     *							if a boolean is true, that means we must update the
     *							corresponding index because changes in the newRow
     *							affect it.
     *	@param  colsToUpdate	array of ints indicating which columns (1 based)
     *							to update.  If null, do all.
     *	@param	tc			transaction controller
     *
     * @exception StandardException		Thrown on failure
     */
    void updateRow( ExecIndexRow				key,
                    ExecRow					newRow,
                    int						indexNumber,
                    boolean[]				indicesToUpdate,
                    int[]					colsToUpdate,
                    TransactionController	tc)
            throws StandardException
    {
        ExecRow[] newRows = new ExecRow[1];
        newRows[0] = newRow;
        updateRow(key, newRows, indexNumber, indicesToUpdate, colsToUpdate, tc);
    }

    /**
     * Updates a set of base rows in a catalog with the same key on an index
     * and updates all the corresponding index rows.
     *
     *	@param	key			key row
     *	@param	newRows		new version of the array of rows
     *	@param	indexNumber	index that key operates
     *	@param	indicesToUpdate	array of booleans, one for each index on the catalog.
     *							if a boolean is true, that means we must update the
     *							corresponding index because changes in the newRow
     *							affect it.
     *	@param  colsToUpdate	array of ints indicating which columns (1 based)
     *							to update.  If null, do all.
     *	@param	tc			transaction controller
     *
     * @exception StandardException		Thrown on failure
     */
    void updateRow( ExecIndexRow				key,
                    ExecRow[]				newRows,
                    int						indexNumber,
                    boolean[]				indicesToUpdate,
                    int[]					colsToUpdate,
                    TransactionController	tc )
            throws StandardException
    {
        ConglomerateController		heapCC;
        ScanController				drivingScan;
        ExecIndexRow	 			drivingIndexRow;
        RowLocation					baseRowLocation;
        ExecRow						baseRow = crf.makeEmptyRow();

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT( indicesToUpdate.length == crf.getNumIndexes(),
                    "Wrong number of indices." );
        }

        RowChanger 					rc  = getRowChanger( tc, colsToUpdate,baseRow );

        // Row level locking
        rc.openForUpdate(indicesToUpdate, TransactionController.MODE_RECORD, true);

		/* Open the heap conglomerate */
        heapCC = tc.openConglomerate(
                getHeapConglomerate(),
                false,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ);

        drivingScan = tc.openScan(
                getIndexConglomerate(indexNumber),  // conglomerate to open
                false, // don't hold open across commit
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_RECORD,
                TransactionController.ISOLATION_REPEATABLE_READ,
                (FormatableBitSet) null,     // all fields as objects
                key.getRowArray(),   // start position - first row
                ScanController.GE,      // startSearchOperation
                null, //scanQualifier
                key.getRowArray(),   // stop position - through last row
                ScanController.GT);     // stopSearchOperation

        // Get an index row based on the base row
        drivingIndexRow = getIndexRowFromHeapRow(
                getIndexRowGenerator( indexNumber ),
                heapCC.newRowLocationTemplate(),
                crf.makeEmptyRow());

        int rowNum = 0;
        while (drivingScan.fetchNext(drivingIndexRow.getRowArray()))
        {
            baseRowLocation = (RowLocation)
                    drivingIndexRow.getColumn(drivingIndexRow.nColumns());
            boolean base_row_exists =
                    heapCC.fetch(
                            baseRowLocation, baseRow, (FormatableBitSet) null);

            if (SanityManager.DEBUG)
            {
                // it can not be possible for heap row to disappear while 
                // holding scan cursor on index at ISOLATION_REPEATABLE_READ.
                SanityManager.ASSERT(base_row_exists, "base row not found");
            }

            rc.updateRow(baseRow, (rowNum == newRows.length - 1) ?
                    newRows[rowNum] : newRows[rowNum++], baseRowLocation );
        }
        rc.finish();
        heapCC.close();
        drivingScan.close();
        rc.close();
    }

    /**
     *	Gets a row changer for this catalog.
     *
     *	@param	tc	transaction controller
     *	@param	changedCols	the columns to change (1 based), may be null
     * @param  baseRow used to detemine column types at creation time
     *         only. The row changer does ***Not*** keep a referance to
     *         this row or change it in any way.
     *
     *	@return	a row changer for this catalog.
     * @exception StandardException		Thrown on failure
     */
    private	RowChanger	getRowChanger( TransactionController tc,
                                        int[] changedCols,
                                        ExecRow baseRow)
            throws StandardException
    {
        RowChanger 					rc;
        int							indexCount = crf.getNumIndexes();
        IndexRowGenerator[]			irgs = new IndexRowGenerator[ indexCount ];
        long[]						cids = new long[ indexCount ];

        if (SanityManager.DEBUG)
        {
            if (changedCols != null)
            {
                for (int i = changedCols.length - 1; i >= 0; i--)
                {
                    SanityManager.ASSERT(changedCols[i] != 0,
                            "Column id is 0, but should be 1 based");
                }
            }
        }

        for ( int ictr = 0; ictr < indexCount; ictr++ )
        {
            irgs[ictr] = getIndexRowGenerator(ictr);
            cids[ictr] = getIndexConglomerate(ictr);
        }

        rc = crf.getExecutionFactory().getRowChanger(getHeapConglomerate(),
                (StaticCompiledOpenConglomInfo) null,
                (DynamicCompiledOpenConglomInfo) null,
                irgs,
                cids,
                (StaticCompiledOpenConglomInfo[]) null,
                (DynamicCompiledOpenConglomInfo[]) null,
                crf.getHeapColumnCount(),
                tc,
                changedCols,
                getStreamStorableHeapColIds(baseRow),
                (Activation) null);
        return	rc;
    }

    private boolean computedStreamStorableHeapColIds = false;
    private int[] streamStorableHeapColIds;
    private int[] getStreamStorableHeapColIds(ExecRow baseRow) throws StandardException
    {
        if (!computedStreamStorableHeapColIds)
        {
            int sshcidLen = 0;
            //
            //Compute the length of streamStorableHeapColIds
            //One entry for each column id.
            DataValueDescriptor[] ra = baseRow.getRowArray();
            for (DataValueDescriptor aRa : ra) if (aRa instanceof StreamStorable) sshcidLen++;

            //
            //If we have some streamStorableHeapColIds we
            //allocate an array to remember them and fill in
            //the array with the 0 based column ids. If we
            //have none leave streamStorableHeapColIds Null.
            if (sshcidLen > 0)
            {
                streamStorableHeapColIds = new int[sshcidLen];
                int sshcidOffset=0;
                for(int ix=0;ix<ra.length;ix++)
                    if (ra[ix] instanceof StreamStorable)
                        streamStorableHeapColIds[sshcidOffset++] = ix;
            }
            computedStreamStorableHeapColIds = true;
        }
        return streamStorableHeapColIds;
    }

    /**
     * Get an index row based on a row from the heap.
     *
     * @param irg		IndexRowGenerator to use
     * @param rl		RowLocation for heap
     * @param heapRow	Row from the heap
     *
     * @return ExecIndexRow	Index row.
     *
     * @exception StandardException		Thrown on error
     */
    private ExecIndexRow getIndexRowFromHeapRow(IndexRowGenerator irg,
                                                RowLocation rl,
                                                ExecRow heapRow)
            throws StandardException
    {
        ExecIndexRow		indexRow;

        indexRow = irg.getIndexRowTemplate();
        // Get an index row based on the base row
        irg.getIndexRow(heapRow, rl, indexRow, (FormatableBitSet) null);

        return indexRow;
    }

    public String toString()
    {
        if (SanityManager.DEBUG)
        {
            return "name: " + this.getTableName() +
                    "\n\theapCongolomerate: "+heapConglomerate +
                    "\n\tnumIndexes: " + ((indexes != null) ? indexes.length : 0) +
                    "\n\tnumIndexesSet: " + numIndexesSet +
                    "\n\theapSet: " + heapSet +
                    "\n";
        }
        else
        {
            return "";
        }
    }
}
