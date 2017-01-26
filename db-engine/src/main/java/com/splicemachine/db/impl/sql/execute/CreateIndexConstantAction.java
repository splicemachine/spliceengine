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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import java.util.Properties;

// Used only to access a debug flag, will be removed or replaced.

/**
 * ConstantAction to create an index either through
 * a CREATE INDEX statement or as a backing index to
 * a constraint.
 */

class CreateIndexConstantAction extends IndexConstantAction
{
    /**
     * Is this for a CREATE TABLE, i.e. it is
     * for a constraint declared in a CREATE TABLE
     * statement that requires a backing index.
     */
    private final boolean forCreateTable;

    private boolean			unique;
    private boolean			uniqueWithDuplicateNulls;
    private String			indexType;
    private String[]		columnNames;
    private boolean[]		isAscending;
    private boolean			isConstraint;
    private UUID			conglomerateUUID;
    private Properties		properties;

    private IndexRowGenerator irg;

    private ExecRow indexTemplateRow;

    /** Conglomerate number for the conglomerate created by this
     * constant action; -1L if this constant action has not been
     * executed.  If this constant action doesn't actually create
     * a new conglomerate--which can happen if it finds an existing
     * conglomerate that satisfies all of the criteria--then this
     * field will hold the conglomerate number of whatever existing
     * conglomerate was found.
     */
    private long conglomId;

    /** Conglomerate number of the physical conglomerate that we
     * will "replace" using this constant action.  That is, if
     * the purpose of this constant action is to create a new physical
     * conglomerate to replace a dropped physical conglomerate, then
     * this field holds the conglomerate number of the dropped physical
     * conglomerate. If -1L then we are not replacing a conglomerate,
     * we're simply creating a new index (and backing physical
     * conglomerate) as normal.
     */
    private long droppedConglomNum;

    // CONSTRUCTORS
    /**
     * 	Make the ConstantAction to create an index.
     * 
     * @param forCreateTable                Being executed within a CREATE TABLE
     *                                      statement
     * @param unique		                True means it will be a unique index
     * @param uniqueWithDuplicateNulls      True means index check and disallow
     *                                      any duplicate key if key has no 
     *                                      column with a null value.  If any 
     *                                      column in the key has a null value,
     *                                      no checking is done and insert will
     *                                      always succeed.
     * @param indexType	                    type of index (BTREE, for example)
     * @param schemaName	                schema that table (and index) 
     *                                      lives in.
     * @param indexName	                    Name of the index
     * @param tableName	                    Name of table the index will be on
     * @param tableId		                UUID of table
     * @param columnNames	                Names of the columns in the index, 
     *                                      in order
     * @param isAscending	                Array of booleans telling asc/desc 
     *                                      on each column
     * @param isConstraint	                TRUE if index is backing up a 
     *                                      constraint, else FALSE
     * @param conglomerateUUID	            ID of conglomerate
     * @param properties	                The optional properties list 
     *                                      associated with the index.
     */
    CreateIndexConstantAction(
            boolean         forCreateTable,
            boolean			unique,
            boolean			uniqueWithDuplicateNulls,
            String			indexType,
            String			schemaName,
            String			indexName,
            String			tableName,
            UUID			tableId,
            String[]		columnNames,
            boolean[]		isAscending,
            boolean			isConstraint,
            UUID			conglomerateUUID,
            Properties		properties)
    {
        super(tableId, indexName, tableName, schemaName);

        this.forCreateTable             = forCreateTable;
        this.unique                     = unique;
        this.uniqueWithDuplicateNulls   = uniqueWithDuplicateNulls;
        this.indexType                  = indexType;
        this.columnNames                = columnNames;
        this.isAscending                = isAscending;
        this.isConstraint               = isConstraint;
        this.conglomerateUUID           = conglomerateUUID;
        this.properties                 = properties;
        this.conglomId                  = -1L;
        this.droppedConglomNum          = -1L;
    }

    /**
     * Make a ConstantAction that creates a new physical conglomerate
     * based on index information stored in the received descriptors.
     * Assumption is that the received ConglomerateDescriptor is still
     * valid (meaning it has corresponding entries in the system tables
     * and it describes some constraint/index that has _not_ been
     * dropped--though the physical conglomerate underneath has).
     *
     * This constructor is used in cases where the physical conglomerate
     * for an index has been dropped but the index still exists. That
     * can happen if multiple indexes share a physical conglomerate but
     * then the conglomerate is dropped as part of "drop index" processing
     * for one of the indexes. (Note that "indexes" here includes indexes
     * which were created to back constraints.) In that case we have to
     * create a new conglomerate to satisfy the remaining sharing indexes,
     * so that's what we're here for.  See ConglomerateDescriptor.drop()
     * for details on when that is necessary.
     */
    CreateIndexConstantAction(ConglomerateDescriptor srcCD,
        TableDescriptor td, Properties properties)
    {
        super(td.getUUID(),
            srcCD.getConglomerateName(), td.getName(), td.getSchemaName());

        this.forCreateTable = false;

        /* We get here when a conglomerate has been dropped and we
         * need to create (or find) another one to fill its place.
         * At this point the received conglomerate descriptor still
         * references the old (dropped) conglomerate, so we can
         * pull the conglomerate number from there.
         */
        this.droppedConglomNum = srcCD.getConglomerateNumber();

        /* Plug in the rest of the information from the received
         * descriptors.
         */
        IndexRowGenerator irg = srcCD.getIndexDescriptor();
        this.unique = irg.isUnique();
        this.uniqueWithDuplicateNulls = irg.isUniqueWithDuplicateNulls();
        this.indexType = irg.indexType();
        this.columnNames = srcCD.getColumnNames();
        this.isAscending = irg.isAscending();
        this.isConstraint = srcCD.isConstraint();
        this.conglomerateUUID = srcCD.getUUID();
        this.properties = properties;
        this.conglomId = -1L;

        /* The ConglomerateDescriptor may not know the names of
         * the columns it includes.  If that's true (which seems
         * to be the more common case) then we have to build the
         * list of ColumnNames ourselves.
         */
        if (columnNames == null)
        {
            int [] baseCols = irg.baseColumnPositions();
            columnNames = new String[baseCols.length];
            ColumnDescriptorList colDL = td.getColumnDescriptorList();
            for (int i = 0; i < baseCols.length; i++)
            {
                columnNames[i] =
                    colDL.elementAt(baseCols[i]-1).getColumnName();
            }
        }
    }
        
    ///////////////////////////////////////////////
    //
    // OBJECT SHADOWS
    //
    ///////////////////////////////////////////////

    public	String	toString()
    {
        // Do not put this under SanityManager.DEBUG - it is needed for
        // error reporting.
        return "CREATE INDEX " + indexName;
    }

    // INTERFACE METHODS


    /**
     *	This is the guts of the Execution-time logic for
     *  creating an index.
     *
     *  <P>
     *  A index is represented as:
     *  <UL>
     *  <LI> ConglomerateDescriptor.
     *  </UL>
     *  No dependencies are created.
     *
     *  @see ConglomerateDescriptor
     *  @see SchemaDescriptor
     *	@see ConstantAction#executeConstantAction
     *
     * @exception StandardException		Thrown on failure
     */
    public void	executeConstantAction( Activation activation )
                        throws StandardException
    {

        throw new RuntimeException("not implemented");
    }

    /**
     * Determines if a statistics entry is to be added for the index.
     * <p>
     * As an optimization, it may be better to not write a statistics entry to
     * SYS.SYSSTATISTICS. If it isn't needed by Derby as part of query
     * optimization there is no reason to spend resources keeping the
     * statistics up to date.
     *
     * @param dd the data dictionary
     * @param irg the index row generator
     * @param numRows the number of rows in the index
     * @return {@code true} if statistics should be written to
     *      SYS.SYSSTATISTICS, {@code false} otherwise.
     * @throws StandardException if accessing the data dictionary fails
     */
    private boolean addStatistics(DataDictionary dd, IndexRowGenerator irg, long numRows) throws StandardException {
		/*
		 * -sf- In splice, we keep statistics up to date using a different mechanism
		 */
		return false;
//        boolean add = (numRows > 0);
//        if (dd.checkVersion(DataDictionary.DD_VERSION_DERBY_10_9, null) &&
//                // This horrible piece of code will hopefully go away soon!
//               ((IndexStatisticsDaemonImpl)dd.getIndexStatsRefresher(false)).
//                    skipDisposableStats) {
//            if (add && irg.isUnique() && irg.numberOfOrderedColumns() == 1) {
//                // Do not add statistics for single-column unique indexes.
//                add = false;
//            }
//        }
//        return add;
    }

    // CLASS METHODS

    ///////////////////////////////////////////////////////////////////////
    //
    //	GETTERs called by CreateConstraint
    //
    ///////////////////////////////////////////////////////////////////////
    ExecRow getIndexTemplateRow()
    {
        return indexTemplateRow;
    }

    /**
     * Get the conglomerate number for the conglomerate that was
     * created by this constant action.  Will return -1L if the
     * constant action has not yet been executed.  This is used
     * for updating conglomerate descriptors which share a
     * conglomerate that has been dropped, in which case those
     * "sharing" descriptors need to point to the newly-created
     * conglomerate (the newly-created conglomerate replaces
     * the dropped one).
     */
    long getCreatedConglomNumber()
    {
        if (SanityManager.DEBUG)
        {
            if (conglomId == -1L)
            {
                SanityManager.THROWASSERT(
                    "Called getCreatedConglomNumber() on a CreateIndex" +
                    "ConstantAction before the action was executed.");
            }
        }

        return conglomId;
    }

    /**
     * If the purpose of this constant action was to "replace" a
     * dropped physical conglomerate, then this method returns the
     * conglomerate number of the dropped conglomerate.  Otherwise
     * this method will end up returning -1.
     */
    long getReplacedConglomNumber()
    {
        return droppedConglomNum;
    }

    /**
     * Get the UUID for the conglomerate descriptor that was created
     * (or re-used) by this constant action.
     */
    UUID getCreatedUUID()
    {
        return conglomerateUUID;
    }

}

