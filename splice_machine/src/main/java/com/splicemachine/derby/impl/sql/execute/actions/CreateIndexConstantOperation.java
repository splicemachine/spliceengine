/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.splicemachine.db.catalog.IndexDescriptor;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.db.iapi.types.HBaseRowLocation;
import com.splicemachine.derby.utils.FormatableBitSetUtils;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnLifecycleManager;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.impl.sql.execute.IndexColumnOrder;
import com.splicemachine.db.impl.sql.execute.RowUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.List;

/**
 * Creates an Index transactionally.
 *
 * Index creation is separated into 3 phases:
 *
 * 1. Metadata
 * 2. Global Create
 * 3. Global populate
 *
 * <h3> Metadata Phase</h3>
 * This phase is where the index elements are added to various system tables. Not
 * much going on here, it's mostly the same as what Derby does--it will look for a
 * duplicate index (e.g. if some other index already covers the data in the proper
 * order), create the conglomerate and so on and so forth.
 *
 * <h3>Create Phase</h3>
 * This phase is where we notify all the other servers in the cluster that the
 * index has been created. We do this by using a child transaction and the
 * task framework to ensure that all nodes have invalidated their Data Dictionary,
 * and that the Write pipeline is notified of the change in the structure.
 *
 * <h3>Populate Phase</h3>
 * This phase is where pre-existing data is indexed properly (and it usually
 * occupies the most time). This occurs within a child transaction.
 *
 * <h3>Transactional State</h3>
 * First, note that the entire operation occurs within the context of an internal
 * transaction, which is committed or aborted based on the output of the three phases.
 * In this way, the behavior of the user transaction is taken out of consideration. Henceforth,
 * we will use the term "parent" to refer to this internal transaction.
 *
 * We have a tricky needle to thread transactionally. On the one hand, we add
 * a filter to the write pipeline so that <em>new</em> transactions (transactions
 * which have a begin timestamp > ours). On the other, we use a populate task
 * to ensure that data which was already present from <em>old</em> transactions will
 * be populated as well.
 *
 * But what about transactions which are created <em>during</em> the process? Consider
 * the following time sequences:
 *
 * Scenario 1: Before Create
 * The sequence is as follows:
 * <ol>
 * <li>Parent transaction is created (Tp = [bp,...))</li>
 * <li>Independent transaction is created (To = [bo,...) such that {@code bp < bo})</li>
 * <li>To commits (To = [bo,co))</li>
 * <li>Create transaction is created as child of parent (Tc = [bc,...),Tc.parent = Tp)</li>
 * <li>Create transaction is committed at {@code cc}(Tc = [bc,cc))</li>
 * <li>Populate transaction is created and committed as child of parent (Tpop = [bpop,cpop),Tpop.parent = Tp)</li>
 * </ol>
 * In this case, we expect that the writes from the independent transaction will not have been
 * picked up by the interceptor that we place during the create phase (as the create phase has not happened yet).
 * Therefore the independent transaction's writes need to be picked up by the populate child transaction--this
 * is a variant on Snapshot isolation--instead of doing a full Snapshot Isolation reading level, we require that
 * data must be committed <em>before</em> the start of the population phase.
 *
 * On the other hand, consider this second scenario:
 *
 * Scenario 2: Between Creation and Population
 * The sequence is:
 *<ol>
 * <li>{@code Tp} created ({@code Tp = [bp,...)}).</li>
 * <li>Create transaction {@code Tc} is created ({@code Tc = [bc,...), Tc.parent = Tp})</li>
 * <li>Create transaction {@code Tc} is committed ({@code Tc = [bc,cc)})</li>
 * <li>Independent Transaction {@code To = [bo,...)} is created such that {@code cc < bo}</li>
 * <li>Independent Transaction {@code To} is committed ({@code To = [bo,co)})</li>
 * <li>Populate Transaction is created and committed.</li>
 *</ol>
 * In this scenario, the independent transaction was created <em>after</em> the create transaction
 * was committed--therefore, we should be able to make the guarantee that the write pipeline interceptor
 * will catch the write and transform it directly; as a result, the populate task <em>cannot</em> see this write.
 *
 * Unfortunately, Scenarios #1 and #2 conflict. In both cases we have an independent transaction which committed
 * before the start of the Populate phase, but in one the writes need to be visible, while in the other they <em>cannot</em>
 * be.
 *
 * We resolve this issue by <em>chaining</em> the create and populate transactions together. Transaction chaining
 * is where we commit one transaction, then create a new transaction whose begin timestamp is the same as the
 * commit timestamp of the committed transaction. For example, we create a transaction at time {@code 1}, then chain
 * commit that transaction. In that case, the commit timestamp if {@code 2}, and the new transaction's begin
 * timestamp is {@code 2}. Thus, there is no possibility of another transaction interleaving with the operation--
 * the transactions are "contiguous in time".
 *
 */
public class CreateIndexConstantOperation extends IndexConstantOperation implements Serializable {
    private static final Logger LOG = Logger.getLogger(CreateIndexConstantOperation.class);
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
    private ExecRow indexTemplateRow;
    private boolean excludeNulls;
    private boolean excludeDefaults;

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

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public CreateIndexConstantOperation(
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
            boolean 		excludeNulls,
            boolean			excludeDefaults,
            Properties		properties) {
        super(tableId, indexName, tableName, schemaName);
        SpliceLogUtils.trace(LOG, "CreateIndexConstantOperation for table %s.%s with index named %s for columns %s",schemaName,tableName,indexName,Arrays.toString(columnNames));
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
        this.excludeDefaults            = excludeDefaults;
        this.excludeNulls               = excludeNulls;
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
    CreateIndexConstantOperation(ConglomerateDescriptor srcCD, TableDescriptor td, Properties properties) {
        super(td.getUUID(),srcCD.getConglomerateName(), td.getName(), td.getSchemaName());
        SpliceLogUtils.trace(LOG, "CreateIndexConstantOperation for conglomerate {%s} and table {%s} with properties {%s}", srcCD,td,properties);
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
        this.excludeNulls = irg.excludeNulls();
        this.excludeDefaults = irg.excludeDefaults();

		/* The ConglomerateDescriptor may not know the names of
		 * the columns it includes.  If that's true (which seems
		 * to be the more common case) then we have to build the
		 * list of ColumnNames ourselves.
		 */
        if (columnNames == null) {
            int [] baseCols = irg.baseColumnPositions();
            columnNames = new String[baseCols.length];
            ColumnDescriptorList colDL = td.getColumnDescriptorList();
            for (int i = 0; i < baseCols.length; i++) {
                columnNames[i] = colDL.elementAt(baseCols[i]-1).getColumnName();
            }
        }
    }



    public	String	toString() {
        return "CREATE INDEX " + indexName;
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
        SpliceLogUtils.trace(LOG, "executeConstantActivation with activation %s",activation);

        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        DependencyManager dm = dd.getDependencyManager();
        TransactionController userTransaction = lcc.getTransactionExecute();

        dd.startWriting(lcc);
        SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, userTransaction, true) ;
        ConglomerateDescriptor existingIndex = dd.getConglomerateDescriptor(indexName, sd, false);
        if (existingIndex != null) {
            throw StandardException.newException(SQLState.LANG_OBJECT_ALREADY_EXISTS_IN_OBJECT,
                    existingIndex.getDescriptorType(),
                    existingIndex.getDescriptorName(),
                    sd.getDescriptorType(),
                    sd.getDescriptorName());
        }
        TableDescriptor td = activation.getDDLTableDescriptor();
        if (td == null) {
            td = tableId != null?dd.getTableDescriptor(tableId):dd.getTableDescriptor(tableName, sd, userTransaction);
        }

        if (td!=null && td.getTableType()==TableDescriptor.EXTERNAL_TYPE) {
            throw StandardException.newException(
                    SQLState.EXTERNAL_TABLES_NO_INDEX,td.getName());
        }



        DDLUtils.validateTableDescriptor(td, indexName, tableName);
        try {
            // invalidate any prepared statements that
            // depended on this table (including this one)
            if (! forCreateTable)
                dm.invalidateFor(td, DependencyManager.CREATE_INDEX, lcc);

            SpliceLogUtils.trace(LOG, "Translation Base Column Names");
            // Translate the base column names to column positions
            IndexRowGenerator			indexRowGenerator = null;
            int[]	baseColumnPositions = new int[columnNames.length];
            int maxBaseColumnPosition = td.getBaseColumnPositions(lcc, baseColumnPositions, columnNames);

            /* The code below tries to determine if the index that we're about
             * to create can "share" a conglomerate with an existing index.
             * If so, we will use a single physical conglomerate--namely, the
             * one that already exists--to support both indexes. I.e. we will
             * *not* create a new conglomerate as part of this constant action.
             */

            // check if we have similar indices already for this table
            ConglomerateDescriptor[] congDescs = td.getConglomerateDescriptors();
            boolean shareExisting = false;
            for (ConglomerateDescriptor cd : congDescs) {
                if (!cd.isIndex())
                    continue;

                if (droppedConglomNum == cd.getConglomerateNumber()) {
                    /* We can't share with any conglomerate descriptor
                     * whose conglomerate number matches the dropped
                     * conglomerate number, because that descriptor's
                     * backing conglomerate was dropped, as well.  If
                     * we're going to share, we have to share with a
                     * descriptor whose backing physical conglomerate
                     * is still around.
                     */
                    continue;
                }

                IndexRowGenerator irg = cd.getIndexDescriptor();
                int[] bcps = irg.baseColumnPositions();
                boolean[] ia = irg.isAscending();
                int j = 0;

                /* The conditions which allow an index to share an existing
                 * conglomerate are as follows:
                 *
                 * 1. the set of columns (both key and include columns) and their
                 *  order in the index is the same as that of an existing index AND
                 *
                 * 2. the ordering attributes are the same AND
                 *
                 * 3. one of the following is true:
                 *    a) the existing index is unique, OR
                 *    b) the existing index is non-unique with uniqueWhenNotNulls
                 *       set to TRUE and the index being created is non-unique, OR
                 *    c) both the existing index and the one being created are
                 *       non-unique and have uniqueWithDuplicateNulls set to FALSE.
                 */
                boolean possibleShare = (irg.isUnique() || !unique) &&
                        (bcps.length == baseColumnPositions.length);

                //check if existing index is non unique and uniqueWithDuplicateNulls
                //is set to true (backing index for unique constraint)
                if (possibleShare && !irg.isUnique()) {
                    /* If the existing index has uniqueWithDuplicateNulls set to
                     * TRUE it can be shared by other non-unique indexes; otherwise
                     * the existing non-unique index has uniqueWithDuplicateNulls
                     * set to FALSE, which means the new non-unique conglomerate
                     * can only share if it has uniqueWithDuplicateNulls set to
                     * FALSE, as well.
                     */
                    possibleShare = (irg.isUniqueWithDuplicateNulls() || !uniqueWithDuplicateNulls);
                }

                if (possibleShare && indexType.equals(irg.indexType())) {
                    for (; j < bcps.length; j++) {
                        if ((bcps[j] != baseColumnPositions[j]) || (ia[j] != isAscending[j]))
                            break;
                    }
                }

                if (j == baseColumnPositions.length) {  // share
                    /*
                     * Don't allow users to create a duplicate index. Allow if being done internally
                     * for a constraint
                     */
                    if (!isConstraint) {
                        activation.addWarning(StandardException.newWarning(SQLState.LANG_INDEX_DUPLICATE, cd.getConglomerateName()));
                        return;
                    }

                    /* Sharing indexes share the physical conglomerate
                     * underneath, so pull the conglomerate number from
                     * the existing conglomerate descriptor.
                     */
                    conglomId = cd.getConglomerateNumber();

                    /* We create a new IndexRowGenerator because certain
                     * attributes--esp. uniqueness--may be different between
                     * the index we're creating and the conglomerate that
                     * already exists.  I.e. even though we're sharing a
                     * conglomerate, the new index is not necessarily
                     * identical to the existing conglomerate. We have to
                     * keep track of that info so that if we later drop
                     * the shared physical conglomerate, we can figure out
                     * what this index (the one we're creating now) is
                     * really supposed to look like.
                     */
                    indexRowGenerator =
                            new IndexRowGenerator(
                                    indexType, unique, uniqueWithDuplicateNulls,
                                    baseColumnPositions,
                                    isAscending,
                                    baseColumnPositions.length,excludeNulls,excludeDefaults);

                    //DERBY-655 and DERBY-1343
                    // Sharing indexes will have unique logical conglomerate UUIDs.
                    conglomerateUUID = dd.getUUIDFactory().createUUID();
                    shareExisting = true;
                    break;
                }
            }

            /* If we have a droppedConglomNum then the index we're about to
             * "create" already exists--i.e. it has an index descriptor and
             * the corresponding information is already in the system catalogs.
             * The only thing we're missing, then, is the physical conglomerate
             * to back the index (because the old conglomerate was dropped).
             */
            boolean alreadyHaveConglomDescriptor = (droppedConglomNum > -1L);

            /* If this index already has an essentially same one, we share the
             * conglomerate with the old one, and just simply add a descriptor
             * entry into SYSCONGLOMERATES--unless we already have a descriptor,
             * in which case we don't even need to do that.
             */
            DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();
            if (shareExisting && !alreadyHaveConglomDescriptor) {
                ConglomerateDescriptor cgd =
                        ddg.newConglomerateDescriptor(conglomId, indexName, true,
                                indexRowGenerator, isConstraint,
                                conglomerateUUID, td.getUUID(), sd.getUUID() );
                dd.addDescriptor(cgd, sd, DataDictionary.SYSCONGLOMERATES_CATALOG_NUM, false, userTransaction);
                // add newly added conglomerate to the list of conglomerate
                // descriptors in the td.
                ConglomerateDescriptorList cdl = td.getConglomerateDescriptorList();
                //noinspection unchecked
                cdl.add(cgd);
                // can't just return yet, need to get member "indexTemplateRow"
                // because create constraint may use it
            }

            long heapConglomerateId = td.getHeapConglomerateId();
            Properties indexProperties = getIndexProperties(baseColumnPositions, heapConglomerateId);
            indexRowGenerator = getIndexRowGenerator(baseColumnPositions, indexRowGenerator, shareExisting);

            // Create the FormatableBitSet for mapping the partial to full base row
            FormatableBitSet bitSet = FormatableBitSetUtils.fromIntArray(td.getNumberOfColumns()+1,baseColumnPositions);
            FormatableBitSet zeroBasedBitSet = RowUtil.shift(bitSet, 1);

            ExecRow baseRow = activation.getExecutionFactory().getValueRow(maxBaseColumnPosition);
            ExecIndexRow indexRow = indexRowGenerator.getIndexRowKeyTemplate();
            ExecRow compactBaseRow = activation.getExecutionFactory().getValueRow(baseColumnPositions.length);

            indexTemplateRow = indexRow;//indexRows[0];

            // Fill the partial row with nulls of the correct type
            ColumnDescriptorList cdl = td.getColumnDescriptorList();
            int	cdlSize = cdl.size();
            DataValueDescriptor defaultValue = null;
            for (int index = 0, numSet = 0; index < cdlSize; index++) {
                if (! zeroBasedBitSet.get(index)) {
                    continue;
                }
                numSet++;
                ColumnDescriptor cd = cdl.elementAt(index);
                DataTypeDescriptor dts = cd.getType();
                if (defaultValue == null && numSet==1) {
                    defaultValue = cd.getDefaultValue();
                }
                DataValueDescriptor colDescriptor = dts.getNull();
                baseRow.setColumn(index+1, colDescriptor);
                compactBaseRow.setColumn(numSet,colDescriptor);
            }
            //convert the base row to an index row
            indexRowGenerator.getIndexRow(compactBaseRow, new HBaseRowLocation(), indexRow, bitSet);

            /* now that we got indexTemplateRow, done for sharing index */
            if (shareExisting) // Sharing leaves...
                return;

            // Put displayable table/index names into properties, which will ultimately be persisted
            // in the HTableDescriptor for convenient fetching where DataDictionary not available.
            indexProperties.setProperty(SIConstants.TABLE_DISPLAY_NAME_ATTR, this.tableName);
            indexProperties.setProperty(SIConstants.INDEX_DISPLAY_NAME_ATTR, this.indexName);

            /* For non-unique indexes, we order by all columns + the RID.
             * For unique indexes, we just order by the columns.
             * We create a unique index observer for unique indexes
             * so that we can catch duplicate key.
             * We create a basic sort observer for non-unique indexes
             * so that we can reuse the wrappers during an external
             * sort.
             */
            conglomId = userTransaction.createConglomerate(td.isExternal(),indexType, indexTemplateRow.getRowArray(),
                    getColumnOrderings(baseColumnPositions), indexRowGenerator.getColumnCollationIds(
                    td.getColumnDescriptorList()), indexProperties, TransactionController.IS_DEFAULT);

            ConglomerateController indexController = userTransaction.openConglomerate(conglomId, false, 0, TransactionController.MODE_TABLE,TransactionController.ISOLATION_SERIALIZABLE);

            // Check to make sure that the conglomerate can be used as an index
            if ( ! indexController.isKeyed()) {
                indexController.close();
                throw StandardException.newException(SQLState.LANG_NON_KEYED_INDEX, indexName,indexType);
            }
            indexController.close();

            //
            // Create a conglomerate descriptor with the conglomId filled
            // in and add it--if we don't have one already.
            //
            if(!alreadyHaveConglomDescriptor){
                long indexCId=createConglomerateDescriptor(dd,userTransaction,sd,td,indexRowGenerator,ddg);
                createAndPopulateIndex(activation,userTransaction,td,indexCId,heapConglomerateId,indexRowGenerator,defaultValue);
            }
        }catch (Throwable t) {
            throw Exceptions.parseException(t);
        }
    }

    public ExecRow getIndexTemplateRow() {
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
    public long getCreatedConglomNumber() {
        if (SanityManager.DEBUG) {
            if (conglomId == -1L) {
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
    public long getReplacedConglomNumber() {
        return droppedConglomNum;
    }

    /**
     * Get the UUID for the conglomerate descriptor that was created
     * (or re-used) by this constant action.
     */
    public UUID getCreatedUUID() {
        return conglomerateUUID;
    }

    @Override protected boolean waitsForConcurrentTransactions() { return true; }

    /****************************************************************************************************************/
    /*private helper methods*/

    private IndexRowGenerator getIndexRowGenerator(int[] baseColumnPositions,
                                                     @Nullable IndexRowGenerator existingGenerator,
                                                     boolean shareExisting) throws StandardException {
        // For now, assume that all index columns are ordered columns
        if (! shareExisting) {
            existingGenerator = new IndexRowGenerator(
                    indexType,
                    unique,
                    uniqueWithDuplicateNulls,
                    baseColumnPositions,
                    isAscending,
                    baseColumnPositions.length,
                    excludeNulls,
                    excludeDefaults);
        }
        return existingGenerator;
    }

    private ColumnOrdering[] getColumnOrderings(int[] baseColumnPositions) {
        ColumnOrdering[] order;
        int numColumnOrderings = baseColumnPositions.length;
        if(!unique)
            numColumnOrderings++;

        order= new ColumnOrdering[numColumnOrderings];
        for (int i=0; i < numColumnOrderings; i++) {
            order[i] = new IndexColumnOrder(i, !(unique || i < numColumnOrderings - 1) || isAscending[i]);
        }
        return order;
    }

    private long createConglomerateDescriptor(DataDictionary dd,
                                                TransactionController tc,
                                                SchemaDescriptor sd,
                                                TableDescriptor td,
                                                IndexRowGenerator indexRowGenerator,
                                                DataDescriptorGenerator ddg) throws StandardException {
            SpliceLogUtils.trace(LOG, "! Conglom Descriptor");

            ConglomerateDescriptor cgd = ddg.newConglomerateDescriptor(conglomId,
                    indexName, true, indexRowGenerator,
                    isConstraint, conglomerateUUID, td.getUUID(), sd.getUUID() );
            dd.addDescriptor(cgd, sd,DataDictionary.SYSCONGLOMERATES_CATALOG_NUM, false, tc);

            // add newly added conglomerate to the list of conglomerate
            // descriptors in the td.
            ConglomerateDescriptorList cdl = td.getConglomerateDescriptorList();
            //noinspection unchecked
            cdl.add(cgd);

            /* Since we created a new conglomerate descriptor, load
             * its UUID into the corresponding field, to ensure that
             * it is properly set in the StatisticsDescriptor created
             * below.
             */
            conglomerateUUID = cgd.getUUID();
            return cgd.getConglomerateNumber();
    }

    private Properties getIndexProperties(int[] baseColumnPositions, long heapConglomerateId) throws StandardException {
        /*
         * Describe the properties of the index to the store using Properties
         *
         * Note that this only works with "BTREE" indices--In Splice language, that
         * is the default "dense, remote" index
         */
        Properties	indexProperties;

        if (properties != null) {
            indexProperties = properties;
        }
        else {
            indexProperties = new Properties();
        }

        // Tell it the conglomerate id of the base table
        indexProperties.put("baseConglomerateId",Long.toString(heapConglomerateId));

        // All indexes are unique because they contain the RowLocation.
        // The number of uniqueness columns must include the RowLocation
        // if the user did not specify a unique index.
        indexProperties.put("nUniqueColumns",
                Integer.toString(unique ? baseColumnPositions.length :
                        baseColumnPositions.length + 1));

        if (uniqueWithDuplicateNulls) {
            // Derby made the distinction between "unique" and "uniqueWithDuplicateNulls"
            // with different behavior for each -- IndexDescriptor.isUnique() returned
            // false when IndexDescriptor.isUniqueWithDuplicateNulls() returned true,
            // for instance.
            // We don't make such a distinction.  Unique is always unique even when
            // uniqueWithDuplicateNulls is true.
            //
            indexProperties.put("uniqueWithDuplicateNulls", Boolean.toString(true));
            unique = true;
        }
        // By convention, the row location column is the last column
        indexProperties.put("rowLocationColumn", Integer.toString(baseColumnPositions.length));

        indexProperties.put("nKeyFields",Integer.toString(baseColumnPositions.length + 1));
        return indexProperties;
    }

    private void createAndPopulateIndex(Activation activation,
                                          TransactionController tc,
                                          TableDescriptor td,
                                          long indexConglomId,
                                          long heapConglomerateId,
                                          IndexDescriptor indexDescriptor,
                                            DataValueDescriptor defaultValue) throws StandardException, IOException {
        /*
         * Manages the Create and Populate index phases
         */
        Txn tentativeTransaction;
        TxnView parentTxn = ((SpliceTransactionManager)tc).getActiveStateTxn();
        try {
            TxnLifecycleManager lifecycleManager = SIDriver.driver().lifecycleManager();

            // tentativeTransaction must be a fully distributed transaction capable of committing with a CommitTimestamp,
            // in order to reuse this commit timestamp as the begin timestamp and chain the transactions
            ((Txn) parentTxn).forbidSubtransactions();
            tentativeTransaction = lifecycleManager.beginChildTransaction(parentTxn, DDLUtils.getIndexConglomBytes(indexConglomId));
        } catch (IOException e) {
            LOG.error("Couldn't start transaction for tentative DDL operation");
            throw Exceptions.parseException(e);
        }
        DDLMessage.DDLChange ddlChange = ProtoUtil.createTentativeIndexChange(tentativeTransaction.getTxnId(),
                activation.getLanguageConnectionContext(),
                td.getHeapConglomerateId(), indexConglomId, td, indexDescriptor, defaultValue);
        String changeId = DDLUtils.notifyMetadataChange(ddlChange);
        tc.prepareDataDictionaryChange(changeId);
        Txn indexTransaction = DDLUtils.getIndexTransaction(tc, tentativeTransaction, td.getHeapConglomerateId(),indexName);
        populateIndex(activation, indexTransaction, tentativeTransaction.getCommitTimestamp(), ddlChange.getTentativeIndex(), td);
        indexTransaction.commit();
    }

}
