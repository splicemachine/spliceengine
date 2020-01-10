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

package com.splicemachine.db.iapi.store.access.conglomerate;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.DynamicCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.store.access.RowLocationRetRowSource;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.services.io.Storable;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;


/**

 A conglomerate is an abstract storage structure (they
 correspond to access methods).  The Conglomerate interface
 corresponds to a single instance of a conglomerate. In
 other words, for each conglomerate in the system, there
 will be one object implementing Conglomerate.
 <P>
 The Conglomerate interface is implemented by each access method.
 The implementation must maintain enough information to properly
 open the conglomerate and scans, and to drop the conglomerate.
 This information typically will include the id of the container
 or containers in which the conglomerate is stored, and my also
 include property information.
 <P>
 Conglomerates are created by a conglomerate factory.  The access
 manager stores them in a directory (which is why they implement
 Storable).

 **/

public interface Conglomerate extends Storable, DataValueDescriptor
{

    /**
     * Add a column to the conglomerate.
     * <p>
     * This routine update's the in-memory object version of the
     * Conglomerate to have one less column
     *
     * Note that not all conglomerates may support this feature.
     *
     * @param xact_manager     The TransactionController under which this
     *                         operation takes place.
     * @param column_id        The column number to remove this column at.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    void dropColumn(
            TransactionManager xact_manager,
            int column_id)
            throws StandardException;


    /**
     * Add a column to the conglomerate.
     * <p>
     * This routine update's the in-memory object version of the 
     * Conglomerate to have one more column of the type described by the
     * input template column.
     *
     * Note that not all conglomerates may support this feature.
     *
     * @param xact_manager     The TransactionController under which this
     *                         operation takes place.
     * @param column_id        The column number to add this column at.
     * @param template_column  An instance of the column to be added to table.
     * @param collation_id     Collation id of the added column.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    void addColumn(
            TransactionManager xact_manager,
            int column_id,
            Storable template_column,
            int collation_id)
            throws StandardException;

    /**
     * Drop this conglomerate.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    void drop(TransactionManager  xact_manager)
            throws StandardException;

    /**
     * Get the containerid of conglomerate.
     * <p>
     * Will have to change when a conglomerate could have more than one 
     * containerid.
     *
     * @return The containerid.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    long getContainerid();

    /**
     * Get the id of the container of the conglomerate.
     * <p>
     * Will have to change when a conglomerate could have more than one 
     * container.  The ContainerKey is a combination of the container id
     * and segment id.
     *
     * @return The ContainerKey.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    long getId();

    /**
     * Return static information about the conglomerate to be included in a
     * a compiled plan.
     * <p>
     * The static info would be valid until any ddl was executed on the 
     * conglomid, and would be up to the caller to throw away when that 
     * happened.  This ties in with what language already does for other 
     * invalidation of static info.  The type of info in this would be 
     * containerid and array of format id's from which templates can be created.
     * The info in this object is read only and can be shared among as many 
     * threads as necessary.
     * <p>
     *
     * @return The static compiled information.
     *
     * @param tc        The TransactionController under which this operation
     *                  takes place.
     * @param conglomId The identifier of the conglomerate to open.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    StaticCompiledOpenConglomInfo getStaticCompiledConglomInfo(
            TransactionController tc,
            long conglomId)
            throws StandardException;

    /**
     * Return dynamic information about the conglomerate to be dynamically 
     * reused in repeated execution of a statement.
     * <p>
     * The dynamic info is a set of variables to be used in a given 
     * ScanController or ConglomerateController.  It can only be used in one 
     * controller at a time.  It is up to the caller to insure the correct 
     * thread access to this info.  The type of info in this is a scratch 
     * template for btree traversal, other scratch variables for qualifier 
     * evaluation, ...
     * <p>
     *
     * @return The dynamic information.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    DynamicCompiledOpenConglomInfo getDynamicCompiledConglomInfo()
            throws StandardException;

    /**
     * Is this conglomerate temporary?
     * <p>
     *
     * @return whether conglomerate is temporary or not.
     **/
    boolean isTemporary();

    /**
     * Bulk load into the conglomerate.
     * <p>
     * Individual rows that are loaded into the conglomerate are not
     * logged. After this operation, the underlying database must be backed up
     * with a database backup rather than an transaction log backup (when we
     * have them). This warning is put here for the benefit of future 
     * generation.
     * <p>
     * @param xact_manager  The TransactionController under which this operation
     *                      takes place.
     *
     * @param createConglom If true, the conglomerate is being created in the
     *                      same operation as the openAndLoadConglomerate.  
     *                      The enables further optimization as recovery does
     *                      not require page allocation to be logged. 
     *
     * @param rowSource     Where the rows come from.
     *
     * @return The number of rows loaded.
     *
     * @exception StandardException Standard exception policy.  If 
     * conglomerage supports uniqueness checks and has been created to 
     * disallow duplicates, and one of the rows being loaded had key columns 
     * which were duplicate of a row already in the conglomerate, then 
     * raise SQLState.STORE_CONGLOMERATE_DUPLICATE_KEY_EXCEPTION.
     *
     **/
    long load(
            TransactionManager xact_manager,
            boolean createConglom,
            RowLocationRetRowSource rowSource)
            throws StandardException;


    /**
     * Open a conglomerate controller.
     * <p>
     *
     * @return The open ConglomerateController.
     *
     * @param xact_manager   The access xact to associate all ops on cc with.
     * @param rawtran        The raw store xact to associate all ops on cc with.
     * @param open_mode      A bit mask of TransactionController.MODE_* bits,
     *                       indicating info about the open.
     * @param lock_level     Either TransactionController.MODE_TABLE or
     *                       TransactionController.MODE_RECORD, as passed into
     *                       the openConglomerate() call.
     *
     * @exception  StandardException  Standard exception policy.
     *
     * @see TransactionController
     **/
    ConglomerateController open(
            TransactionManager              xact_manager,
            Transaction                     rawtran,
            boolean                         hold,
            int                             open_mode,
            int                             lock_level,
            StaticCompiledOpenConglomInfo   static_info,
            DynamicCompiledOpenConglomInfo  dynamic_info)
            throws StandardException;

    /**
     * Open a scan controller.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    ScanManager openScan(
            TransactionManager              xact_manager,
            Transaction                     rawtran,
            boolean                         hold,
            int                             open_mode,
            int                             lock_level,
            int                             isolation_level,
            FormatableBitSet				scanColumnList,
            DataValueDescriptor[]	        startKeyValue,
            int                             startSearchOperator,
            Qualifier                       qualifier[][],
            DataValueDescriptor[]           stopKeyValue,
            int                             stopSearchOperator,
            StaticCompiledOpenConglomInfo   static_info,
            DynamicCompiledOpenConglomInfo  dynamic_info)
            throws StandardException;

    void purgeConglomerate(
            TransactionManager              xact_manager,
            Transaction                     rawtran)
            throws StandardException;

    void compressConglomerate(
            TransactionManager              xact_manager,
            Transaction                     rawtran)
            throws StandardException;

}
