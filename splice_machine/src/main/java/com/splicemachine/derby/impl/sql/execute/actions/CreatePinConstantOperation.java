/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.ScanOperation;
import com.splicemachine.derby.impl.sql.execute.pin.DistributedPopulatePinJob;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.derby.stream.iapi.ScopeNamed;
import com.splicemachine.derby.stream.output.WriteReadUtils;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;

/**
 * Pin the table into columnar storage
 *
 */
public class CreatePinConstantOperation implements ConstantAction, ScopeNamed {
    private static final Logger LOG = Logger.getLogger(DDLConstantOperation.class);
    private final String schemaName;
    private final String tableName;

    // CONSTRUCTORS
    /**
     * 	Make the ConstantAction to create an index.
     *
     * @param schemaName	                schema that table (and index)
     *                                      lives in.
     * @param tableName	                    Name of table the index will be on
     *                                      associated with the index.
     */

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public CreatePinConstantOperation(
            String			schemaName,
            String			tableName) {
        SpliceLogUtils.trace(LOG, "CreatePinConstantOperation for schema table pattern %s.%s",schemaName,tableName);
        this.schemaName = schemaName;
        this.tableName = tableName;
    }

    /**
     *
     * Print out string representation
     *
     * @return
     */
    public	String	toString() {
        if (tableName == null)
            return "PIN SCHEMA " + schemaName;
        return String.format("PIN TABLE %s.%s",schemaName,tableName);
    }

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
    public void executeConstantAction( Activation activation ) throws StandardException {
        SpliceLogUtils.trace(LOG, "executeConstantActivation with activation %s", activation);

        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        TransactionController userTransaction = lcc.getTransactionExecute();
        SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, userTransaction, true);
        TableDescriptor td = dd.getTableDescriptor(tableName, sd, userTransaction);
        if (td == null) {
            throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
        }

        DistributedDataSetProcessor dsp = EngineDriver.driver().processorFactory().distributedProcessor();
        TxnView parentTxn = ((SpliceTransactionManager)userTransaction).getActiveStateTxn();
        SpliceConglomerate conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) activation.getTransactionController()).findConglomerate(td.getHeapConglomerateId());
        int[] baseColumnMap = IntArrays.count(conglomerate.getFormat_ids().length);

        FormatableBitSet accessedCols = new FormatableBitSet(conglomerate.getFormat_ids().length);
        for (int i = 0; i < conglomerate.getFormat_ids().length; i++) {
            accessedCols.set(i);
        }

        FormatableBitSet accessedKeyCols = new FormatableBitSet(conglomerate.getColumnOrdering().length);
        for (int i = 0; i < conglomerate.getColumnOrdering().length; i++) {
            accessedKeyCols.set(i);
        }
        ScanSetBuilder<LocatedRow> builder = dsp.newScanSet(null,Long.toString(td.getHeapConglomerateId()));
            builder.tableDisplayName(tableName)
                .transaction(parentTxn)
                .scan(DDLUtils.createFullScan())
                .keyColumnEncodingOrder(conglomerate.getColumnOrdering())
                .reuseRowLocation(false)
                .keyColumnSortOrder(conglomerate.getAscDescInfo())
                .baseColumnMap(baseColumnMap)
                .keyColumnTypes(ScanOperation.getKeyFormatIds(conglomerate.getColumnOrdering(),
                        conglomerate.getFormat_ids()
                ))
                .keyDecodingMap(ScanOperation.getKeyDecodingMap(accessedKeyCols,
                        conglomerate.getColumnOrdering(),
                        baseColumnMap
                ))
                    .rowDecodingMap(ScanOperation.getRowDecodingMap(accessedKeyCols,conglomerate.getColumnOrdering(),baseColumnMap))
                .accessedKeyColumns(accessedKeyCols)
                .template(td.getEmptyExecRow())
                .escaped(td.getEscaped())
                .lines(td.getLines())
                .delimited(td.getDelimited())
                .location(td.getLocation())
                .storedAs(td.getStoredAs());
        String scope = this.getScopeName();
        String prefix = StreamUtils.getScopeString(this);
        String userId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
        String jobGroup = userId + " <" +parentTxn.getTxnId() +">";
        try {
            EngineDriver.driver().getOlapClient().execute(new DistributedPopulatePinJob(builder, scope, jobGroup, prefix, conglomerate.getContainerid()));
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    @Override
    public String getScopeName() {
        return "CreatePinConstantOperation";
    }
}
