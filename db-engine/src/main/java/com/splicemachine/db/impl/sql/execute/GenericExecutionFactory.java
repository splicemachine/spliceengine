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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.execute;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.EngineType;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.services.monitor.ModuleControl;
import com.splicemachine.db.iapi.services.monitor.ModuleSupportable;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.iapi.sql.execute.*;
import com.splicemachine.db.iapi.store.access.DynamicCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.ListDataType;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.GenericResultDescription;

import java.util.Properties;
import java.util.Vector;

/**
 * This Factory is for creating the execution items needed by a connection for a given database.  Once created for
 * the connection, they should be pushed onto the execution context so that they can be found again by subsequent
 * actions during the session.
 */
public abstract class GenericExecutionFactory implements ModuleControl, ModuleSupportable, ExecutionFactory {

    //
    // ModuleControl interface
    //
    @Override
    public boolean canSupport(Properties startParams) {
        return Monitor.isDesiredType(startParams, EngineType.STANDALONE_DB);
    }

    /**
     * This Factory is expected to be booted relative to a LanguageConnectionFactory.
     *
     * @see com.splicemachine.db.iapi.sql.conn.LanguageConnectionFactory
     */
    @Override
    public void boot(boolean create, Properties startParams) throws StandardException {
        // do we need to/ is there some way to check that we are configured per database?
        /* Creation of the connection execution factories  for this database deferred until needed to reduce boot time.*/
        // REMIND: removed boot of LanguageFactory because that is done in BasicDatabase.
    }

    @Override
    public void stop() {
    }

    //
    // ExecutionFactory interface
    //

    /**
     * Factories are generic and can be used by all connections.
     * We defer instantiation until needed to reduce boot time.
     * We may instantiate too many instances in rare multi-user
     * situation, but consistency will be maintained and at some
     * point, usually always, we will have 1 and only 1 instance
     * of each factory because assignment is atomic.
     */
    @Override
    public ResultSetFactory getResultSetFactory() throws StandardException {
        if (rsFactory == null) {
            rsFactory = (ResultSetFactory) Monitor.bootServiceModule(false, this, ResultSetFactory.MODULE, null);
        }
        return rsFactory;
    }

    /**
     * Get the factory for constant actions.
     *
     * @return the factory for constant actions.
     */
    public abstract GenericConstantActionFactory getConstantActionFactory();

    /**
     * We want a dependency context so that we can push it onto
     * the stack.  We could instead require the implementation
     * push it onto the stack for us, but this way we know
     * which context object exactly was pushed onto the stack.
     */
    @Override
    public ExecutionContext newExecutionContext(ContextManager cm) {
        /* Pass in nulls for execution factories.  GEC
         * will call back to get factories when needed.
		 * This allows us to reduce boot time class loading.
		 * (Replication currently instantiates factories
		 * at boot time.)
		 */
        return new GenericExecutionContext(cm, this);
    }

    @Override
    public ScanQualifier[][] getScanQualifier(int numQualifiers) {
        ScanQualifier[] sqArray = new GenericScanQualifier[numQualifiers];
        for (int ictr = 0; ictr < numQualifiers; ictr++) {
            sqArray[ictr] = new GenericScanQualifier();
        }
        return new ScanQualifier[][]{sqArray};
    }

    /**
     * Make a result description
     */
    @Override
    public ResultDescription getResultDescription(ResultColumnDescriptor[] columns, String statementType) {
        return new GenericResultDescription(columns, statementType);
    }

    /**
     * Create an execution time ResultColumnDescriptor from a compile time RCD.
     *
     * @param compileRCD The compile time RCD.
     * @return The execution time ResultColumnDescriptor
     */
    @Override
    public ResultColumnDescriptor getResultColumnDescriptor(ResultColumnDescriptor compileRCD) {
        return new GenericColumnDescriptor(compileRCD);
    }

    /**
     * @see ExecutionFactory#releaseScanQualifier
     */
    @Override
    public void releaseScanQualifier(ScanQualifier[][] qualifiers) {
    }

    /**
     * @see ExecutionFactory#getQualifier
     */
    @Override
    public Qualifier getQualifier(int columnId,
                                  int storagePosition,
                                  int operator,
                                  GeneratedMethod orderableGetter,
                                  Activation activation,
                                  boolean orderedNulls,
                                  boolean unknownRV,
                                  boolean negateCompareResult,
                                  int variantType) {
        return new GenericQualifier(columnId, storagePosition, operator, orderableGetter,
                activation, orderedNulls, unknownRV,
                negateCompareResult, variantType);
    }

    /**
     * @see ExecutionFactory#getQualifier
     */
    @Override
    public Qualifier getQualifier(int columnId,
                                  int storagePosition,
                                  int operator,
                                  GeneratedMethod orderableGetter,
                                  Activation activation,
                                  boolean orderedNulls,
                                  boolean unknownRV,
                                  boolean negateCompareResult,
                                  int variantType,
                                  String name) {
        return new GenericQualifier(columnId, storagePosition, operator, orderableGetter,
                activation, orderedNulls, unknownRV,
                negateCompareResult, variantType, name);
    }

    /**
     * @see ExecutionFactory#getRowChanger
     */
    @Override
    public RowChanger getRowChanger(long heapConglom,
                                    StaticCompiledOpenConglomInfo heapSCOCI,
                                    DynamicCompiledOpenConglomInfo heapDCOCI,
                                    IndexRowGenerator[] irgs,
                                    long[] indexCIDS,
                                    StaticCompiledOpenConglomInfo[] indexSCOCIs,
                                    DynamicCompiledOpenConglomInfo[] indexDCOCIs,
                                    int numberOfColumns,
                                    TransactionController tc,
                                    int[] changedColumnIds,
                                    int[] streamStorableHeapColIds,
                                    Activation activation) throws StandardException {
        return new RowChangerImpl(heapConglom,
                heapSCOCI, heapDCOCI,
                irgs, indexCIDS, indexSCOCIs, indexDCOCIs,
                numberOfColumns,
                changedColumnIds, tc, null,
                streamStorableHeapColIds, activation);
    }

    /**
     * @see ExecutionFactory#getRowChanger
     */
    @Override
    public RowChanger getRowChanger(long heapConglom,
                                    StaticCompiledOpenConglomInfo heapSCOCI,
                                    DynamicCompiledOpenConglomInfo heapDCOCI,
                                    IndexRowGenerator[] irgs,
                                    long[] indexCIDS,
                                    StaticCompiledOpenConglomInfo[] indexSCOCIs,
                                    DynamicCompiledOpenConglomInfo[] indexDCOCIs,
                                    int numberOfColumns,
                                    TransactionController tc,
                                    int[] changedColumnIds,
                                    FormatableBitSet baseRowReadList,
                                    int[] baseRowReadMap,
                                    int[] streamStorableColIds,
                                    Activation activation) throws StandardException {
        return new RowChangerImpl(heapConglom,
                heapSCOCI, heapDCOCI,
                irgs, indexCIDS, indexSCOCIs, indexDCOCIs,
                numberOfColumns,
                changedColumnIds, tc, baseRowReadList,
                baseRowReadMap, activation);
    }


    /**
     * Get a trigger execution context
     *
     * @throws StandardException Thrown on error
     */
    public TriggerExecutionContext getTriggerExecutionContext(String statementText,
                                                              int[] changedColIds,
                                                              String[] changedColNames,
                                                              UUID targetTableId,
                                                              String targetTableName,
                                                              Vector<AutoincrementCounter> aiCounters) throws StandardException {
        return new TriggerExecutionContext(statementText,
                changedColIds,
                changedColNames,
                targetTableId,
                targetTableName,
                aiCounters,
                null);
    }

    public TriggerExecutionContext getTriggerExecutionContext(String statementText,
                                                              int[] changedColIds,
                                                              String[] changedColNames,
                                                              UUID targetTableId,
                                                              String targetTableName,
                                                              Vector<AutoincrementCounter> aiCounters,
                                                              FormatableBitSet heapList) throws StandardException {
        return new TriggerExecutionContext(statementText,
                changedColIds,
                changedColNames,
                targetTableId,
                targetTableName,
                aiCounters,
                heapList);
    }
    /*
        Old RowFactory interface
     */
    @Override
    public ExecRow getValueRow(int numColumns) {
        return new ValueRow(numColumns);
    }

    @Override
    public ExecIndexRow getIndexableRow(int numColumns) {
        return new IndexRow(numColumns);
    }

    @Override
    public ExecIndexRow getIndexableRow(ExecRow valueRow) {
        if (valueRow instanceof ExecIndexRow) {
            return (ExecIndexRow) valueRow;
        }
        return new IndexValueRow(valueRow);
    }
    
    @Override
    public ListDataType getListData(int numberOfValues) {
        return new ListDataType(numberOfValues);
    }

    //
    // class interface
    //
    public GenericExecutionFactory() {
    }

    //
    // fields
    //
    private ResultSetFactory rsFactory;

}
