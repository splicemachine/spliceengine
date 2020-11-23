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

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.GroupFetchScanController;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 * This class describes actions that are ALWAYS performed for a
 * constraint creation at Execution time.
 *
 * @version 0.1
 */
public abstract class ConstraintConstantAction extends DDLSingleTableConstantAction
{

    protected String   constraintName;
    protected int    constraintType;
    protected String   tableName;
    protected String   schemaName;
    protected UUID   schemaId;
    protected  ConstantAction indexAction;

    // CONSTRUCTORS
    /**
     * Make one of these puppies.
     *
     *  @param constraintName Constraint name.
     *  @param constraintType Constraint type.
     *  @param tableName  Table name.
     *  @param tableId   UUID of table.
     *  @param schemaName  schema that table and constraint lives in.
     *  @param indexAction  IndexConstantAction for constraint (if necessary)
     *  RESOLVE - the next parameter should go away once we use UUIDs
     *     (Generated constraint names will be based off of uuids)
     */
    public ConstraintConstantAction(
                       String constraintName,
                       int  constraintType,
                       String tableName,
                       UUID  tableId,
                       String schemaName,
                       ConstantAction indexAction)
    {
        super(tableId);
        this.constraintName = constraintName;
        this.constraintType = constraintType;
        this.tableName = tableName;
        this.indexAction = indexAction;
        this.schemaName = schemaName;

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(schemaName != null, "Constraint schema name is null");
        }
    }

    // Class implementation

    /**
     * Get the constraint type.
     *
     * @return The constraint type
     */
    public int getConstraintType()
    {
        return constraintType;
    }

    /**
      * Get the constraint name
      *
      * @return the constraint name
      */
    public String getConstraintName() { return constraintName; }

    /**
      * Get the associated index constant action.
      *
      * @return the constant action for the backing index
      */
    public ConstantAction getIndexAction() { return indexAction; }

    /**
     * Make sure that the foreign key constraint is valid
     * with the existing data in the target table.  Open
     * the table, if there aren't any rows, ok.  If there
     * are rows, open a scan on the referenced key with
     * table locking at level 2.  Pass in the scans to
     * the BulkRIChecker.  If any rows fail, barf.
     *
     * @param tc  transaction controller
     * @param dd  data dictionary
     * @param fk  foreign key constraint
     * @param refcd referenced key
     * @param  indexTemplateRow index template row
     *
     * @exception StandardException on error
     */
    public static void validateFKConstraint
    (
        TransactionController    tc,
        DataDictionary      dd,
        ForeignKeyConstraintDescriptor  fk,
        ReferencedKeyConstraintDescriptor refcd,
        ExecRow        indexTemplateRow
    )
        throws StandardException
    {

        GroupFetchScanController refScan = null;

        GroupFetchScanController fkScan =
                tc.openGroupFetchScan(
                        fk.getIndexConglomerateDescriptor(dd).getConglomerateNumber(),
                        false,                          // hold
                        0,           // read only
                        TransactionController.MODE_TABLE,       // already locked
                        TransactionController.ISOLATION_READ_COMMITTED,   // whatever
                        (FormatableBitSet) null,        // retrieve all fields
                        (DataValueDescriptor[]) null,         // startKeyValue
                        ScanController.GE,               // startSearchOp
                        null,                            // qualifier
                        (DataValueDescriptor[]) null,     // stopKeyValue
                        ScanController.GT                // stopSearchOp
                );

        try
        {
            /*
            ** If we have no rows, then we are ok.  This will
            ** catch the CREATE TABLE T (x int references P) case
            ** (as well as an ALTER TABLE ADD CONSTRAINT where there
            ** are no rows in the target table).
            */
            if (!fkScan.next())
            {
                fkScan.close();
                return;
            }

            fkScan.reopenScan(
                    (DataValueDescriptor[])null,      // startKeyValue
                    ScanController.GE,               // startSearchOp
                    null,                            // qualifier
                    (DataValueDescriptor[])null,     // stopKeyValue
                    ScanController.GT                // stopSearchOp
                    );

            /*
            ** Make sure each row in the new fk has a matching
            ** referenced key.  No need to get any special locking
            ** on the referenced table because it cannot delete
            ** any keys we match because it will block on the table
            ** lock on the fk table (we have an ex tab lock on
            ** the target table of this ALTER TABLE command).
            ** Note that we are doing row locking on the referenced
            ** table.  We could speed things up and get table locking
            ** because we are likely to be hitting a lot of rows
            ** in the referenced table, but we are going to err
            ** on the side of concurrency here.
            */
            refScan =
                tc.openGroupFetchScan(
                    refcd.getIndexConglomerateDescriptor(dd).getConglomerateNumber(),
                        false,                        // hold 
                        0,         // read only
                        TransactionController.MODE_RECORD,
                        TransactionController.ISOLATION_READ_COMMITTED, // read committed is good enough
                        (FormatableBitSet)null,      // retrieve all fields
                        (DataValueDescriptor[])null,    // startKeyValue
                        ScanController.GE,             // startSearchOp
                        null,                          // qualifier
                        (DataValueDescriptor[])null,   // stopKeyValue
                        ScanController.GT              // stopSearchOp 
                        );

            RIBulkChecker riChecker = new RIBulkChecker(refScan,
                                        fkScan,
                                        indexTemplateRow,
                                        true,     // fail on 1st failure
                                        (ConglomerateController)null,
                                        (ExecRow)null);

            int numFailures = riChecker.doCheck();
            if (numFailures > 0)
            {
                throw StandardException.newException(SQLState.LANG_ADD_FK_CONSTRAINT_VIOLATION,
                                    fk.getConstraintName(),
                                    fk.getTableDescriptor().getName());
            }
        }
        finally
        {
            if (fkScan != null)
            {
                fkScan.close();
                fkScan = null;
            }
            if (refScan != null)
            {
                refScan.close();
                refScan = null;
            }
        }
    }
}
