package com.splicemachine.derby.impl.store.access.hbase;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;
import com.google.common.io.Closeables;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.StatsStoreCostController;
import com.splicemachine.derby.utils.ConglomerateUtils;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.Txn;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.cache.ClassSize;
import com.splicemachine.db.iapi.services.io.ArrayInputStream;
import com.splicemachine.db.iapi.services.io.FormatIdUtil;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.io.Storable;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.store.access.ColumnOrdering;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.DynamicCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.store.access.RowLocationRetRowSource;
import com.splicemachine.db.iapi.store.access.RowUtil;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;
import com.splicemachine.db.iapi.store.access.conglomerate.ScanManager;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.ContainerHandle;
import com.splicemachine.db.iapi.store.raw.ContainerKey;
import com.splicemachine.db.iapi.store.raw.LockingPolicy;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.store.access.conglomerate.ConglomerateUtil;
import com.splicemachine.db.impl.store.access.conglomerate.OpenConglomerateScratchSpace;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceScan;
import com.splicemachine.utils.SpliceUtilities;

/**
 * A hbase object corresponds to an instance of a hbase conglomerate.  
 *
 *
 **/

public class HBaseConglomerate extends SpliceConglomerate {
    public static final long serialVersionUID = 5l;
    protected static Logger LOG = Logger.getLogger(HBaseConglomerate.class);

    public HBaseConglomerate() {
        super();
    }

    protected void create(
            Transaction             rawtran,
            int                     segmentId,
            long                    input_containerid,
            DataValueDescriptor[]   template,
            ColumnOrdering[]        columnOrder,
            int[]                   collationIds,
            Properties              properties,
            int                     conglom_format_id,
            int                     tmpFlag) throws StandardException {
        super.create(rawtran, segmentId, input_containerid, template, columnOrder, collationIds, properties, conglom_format_id, tmpFlag);
        //elevate the transaction
//        ((SpliceTransaction)rawtran).elevate(Bytes.toBytes(Long.toString(containerId)));
        ConglomerateUtils.createConglomerate(containerId,this, ((SpliceTransaction)rawtran).getTxn());
    }

	/*
	** Methods of Conglomerate
	*/

    /**
     * Add a column to the hbase conglomerate.
     * <p>
     * This routine update's the in-memory object version of the HBase
     * Conglomerate to have one more column of the type described by the
     * input template column.
     *
     * @param column_id        The column number to add this column at.
     * @param template_column  An instance of the column to be added to table.
     * @param collation_id     Collation id of the column added.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    public void addColumn(TransactionManager xact_manager, int column_id, Storable template_column, int collation_id) throws StandardException {
        SpliceLogUtils.trace(LOG, "addColumn column_id=%s, template_column=%s, table_nam=%s", column_id, template_column, getContainerid());
        Table htable = null;
        try {
            htable = SpliceAccessManager.getHTable(getContainerid());
            int[] old_format_ids = format_ids;
            format_ids = new int[old_format_ids.length + 1];
            System.arraycopy(old_format_ids, 0, format_ids, 0, old_format_ids.length);

            // add the new column
            format_ids[old_format_ids.length] = template_column.getTypeFormatId();

            // create a new collation array, and copy old values to it.
            int[] old_collation_ids = collation_ids;
            collation_ids = new int[old_collation_ids.length + 1];
            System.arraycopy(old_collation_ids, 0, collation_ids, 0, old_collation_ids.length);
            // add the new column's collation id.
            collation_ids[old_collation_ids.length] =  collation_id;
            ConglomerateUtils.updateConglomerate(this, (Txn)((SpliceTransactionManager)xact_manager).getActiveStateTxn());
        } catch (StandardException e) {
            SpliceLogUtils.logAndThrow(LOG,"exception in HBaseConglomerate#addColumn",e);
        } finally {
            Closeables.closeQuietly(htable);
        }
    }


    /**
     Drop this hbase conglomerate (what's the relationship with dropping container).
     @see Conglomerate#drop

     @exception StandardException Standard exception policy.
     **/
    public void drop(TransactionManager xact_manager) throws StandardException {
        SpliceLogUtils.trace(LOG, "drop with account manager %s",xact_manager);
        try {
            SpliceUtilities.deleteTable(SpliceUtilities.getAdmin(), id.getContainerId());
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    public boolean fetchMaxOnBTree(
            TransactionManager      xact_manager,
            Transaction             rawtran,
            long                    conglomId,
            int                     open_mode,
            int                     lock_level,
            LockingPolicy           locking_policy,
            int                     isolation_level,
            FormatableBitSet                 scanColumnList,
            DataValueDescriptor[]   fetchRow)
            throws StandardException
    {
        if (LOG.isTraceEnabled())
            LOG.trace("fetchMaxOnBTree ");

        // no support for max on a hbase table.
        throw(StandardException.newException(
                SQLState.HEAP_UNIMPLEMENTED_FEATURE));
    }

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
    public DynamicCompiledOpenConglomInfo getDynamicCompiledConglomInfo()
            throws StandardException
    {
        if (LOG.isTraceEnabled())
            LOG.trace("getDynamicCompiledConglomInfo ");
        //FIXME: do we need this
        return(new OpenConglomerateScratchSpace(
                format_ids, collation_ids, hasCollatedTypes));
    }

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
     * @param conglomId The identifier of the conglomerate to open.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    public StaticCompiledOpenConglomInfo getStaticCompiledConglomInfo(
            TransactionController   tc,
            long                    conglomId)
            throws StandardException
    {
        if (LOG.isTraceEnabled())
            LOG.trace("getStaticCompiledConglomInfo ");
        return(this);
    }


    /**
     * Bulk load into the conglomerate.
     * <p>
     *
     * @see Conglomerate#load
     *
     * @exception  StandardException  Standard exception policy.
     **/
    public long load(
            TransactionManager      xact_manager,
            boolean                 createConglom,
            RowLocationRetRowSource rowSource)
            throws StandardException {
        SpliceLogUtils.trace(LOG, "load rowSourc %s", rowSource);
        long num_rows_loaded = 0;
        return(num_rows_loaded);
    }

    /**
     * Open a hbase controller.
     * <p>
     *
     * @see Conglomerate#open
     *
     * @exception  StandardException  Standard exception policy.
     **/
    public ConglomerateController open(
            TransactionManager              xact_manager,
            Transaction                     rawtran,
            boolean                         hold,
            int                             open_mode,
            int                             lock_level,
            LockingPolicy                   locking_policy,
            StaticCompiledOpenConglomInfo   static_info,
            DynamicCompiledOpenConglomInfo  dynamic_info) throws StandardException {
        SpliceLogUtils.trace(LOG, "open conglomerate id: %d",id.getContainerId());
        OpenSpliceConglomerate open_conglom = new OpenSpliceConglomerate(xact_manager,rawtran,hold,open_mode,lock_level,locking_policy,static_info,dynamic_info,this);
        return new HBaseController(open_conglom, rawtran);
    }

    /**
     * Open a hbase scan controller.
     * <p>
     *
     * @see Conglomerate#openScan
     *
     * @exception  StandardException  Standard exception policy.
     **/
    public ScanManager openScan(
            TransactionManager              xact_manager,
            Transaction                     rawtran,
            boolean                         hold,
            int                             open_mode,
            int                             lock_level,
            LockingPolicy                   locking_policy,
            int                             isolation_level,
            FormatableBitSet				scanColumnList,
            DataValueDescriptor[]	        startKeyValue,
            int                             startSearchOperator,
            Qualifier                       qualifier[][],
            DataValueDescriptor[]	        stopKeyValue,
            int                             stopSearchOperator,
            StaticCompiledOpenConglomInfo   static_info,
            DynamicCompiledOpenConglomInfo  dynamic_info)
            throws StandardException
    {
        SpliceLogUtils.trace(LOG, "open scan: %s", id);
        if (!RowUtil.isRowEmpty(startKeyValue) || !RowUtil.isRowEmpty(stopKeyValue))
            throw StandardException.newException(SQLState.HEAP_UNIMPLEMENTED_FEATURE);
        OpenSpliceConglomerate open_conglom = new OpenSpliceConglomerate(xact_manager,rawtran,hold,open_mode,
                lock_level,locking_policy, static_info, dynamic_info,this);
        SpliceScan hbasescan = new SpliceScan(open_conglom,scanColumnList,startKeyValue,startSearchOperator,
                qualifier,stopKeyValue,stopSearchOperator,rawtran,false);
        return(hbasescan);
    }

    public void purgeConglomerate(TransactionManager xact_manager,Transaction rawtran) throws StandardException {
        SpliceLogUtils.trace(LOG,"purgeConglomerate: %s", id);
    }

    public void compressConglomerate(TransactionManager xact_manager,Transaction rawtran) throws StandardException {
        SpliceLogUtils.trace(LOG,"compressConglomerate: %s", id);
    }

    /**
     * Open a hbase compress scan.
     * <p>
     *
     * @see Conglomerate#defragmentConglomerate
     *
     * @exception  StandardException  Standard exception policy.
     **/
    public ScanManager defragmentConglomerate(
            TransactionManager              xact_manager,
            Transaction                     rawtran,
            boolean                         hold,
            int                             open_mode,
            int                             lock_level,
            LockingPolicy                   locking_policy,
            int                             isolation_level) throws StandardException {
        SpliceLogUtils.trace(LOG, "defragmentConglomerate: %s", id);
        return null;
    }


    /**
     * Return an open StoreCostController for the conglomerate.
     * <p>
     * Return an open StoreCostController which can be used to ask about
     * the estimated row counts and costs of ScanController and
     * ConglomerateController operations, on the given conglomerate.
     * <p>
     * @param xact_manager The TransactionController under which this
     *                     operation takes place.
     * @param rawtran  raw transaction context in which scan is managed.
     *
     * @return The open StoreCostController.
     *
     * @exception  StandardException  Standard exception policy.
     *
     * @see StoreCostController
     **/
    public StoreCostController openStoreCost(ConglomerateDescriptor cd,
                                             TransactionManager xact_manager,
                                             Transaction rawtran) throws StandardException{
        OpenSpliceConglomerate open_conglom = new OpenSpliceConglomerate(xact_manager,rawtran,false,
                ContainerHandle.MODE_READONLY,
                TransactionController.MODE_TABLE, null, null, null, this);
        return new StatsStoreCostController(open_conglom);
    }

    /**************************************************************************
     * Public Methods of StaticCompiledOpenConglomInfo Interface:
     **************************************************************************
     */

    /**
     * return the "Conglomerate".
     * <p>
     * For hbase just return "this", which both implements Conglomerate and
     * StaticCompiledOpenConglomInfo.
     * <p>
     *
     * @return this
     **/
    public DataValueDescriptor getConglom() {
        SpliceLogUtils.trace(LOG, "getConglom: %s",id);
        return(this);
    }


    /**************************************************************************
     * Methods of Storable (via Conglomerate)
     * Storable interface, implies Externalizable, TypedFormat
     **************************************************************************
     */

    /**
     * Return my format identifier.
     *
     * @see com.splicemachine.db.iapi.services.io.TypedFormat#getTypeFormatId
     **/
    public int getTypeFormatId() {
        return StoredFormatIds.ACCESS_HEAP_V3_ID;
    }

    /**
     * Store the stored representation of column value in stream.
     * <p>
     * This routine uses the current database version to either store the
     * the 10.2 format (ACCESS_HEAP_V2_ID) or the current format
     * (ACCESS_HEAP_V3_ID).
     * <p>
     **/
    public void writeExternal(ObjectOutput out) throws IOException {
        FormatIdUtil.writeFormatIdInteger(out, conglom_format_id);
        FormatIdUtil.writeFormatIdInteger(out, tmpFlag);
        out.writeInt((int) id.getSegmentId());
        out.writeLong(id.getContainerId());
        out.writeInt(format_ids.length);
        ConglomerateUtil.writeFormatIdArray(format_ids, out);
        out.writeInt(collation_ids.length);
        ConglomerateUtil.writeFormatIdArray(collation_ids, out);
        out.writeInt(columnOrdering.length);
        ConglomerateUtil.writeFormatIdArray(columnOrdering, out);
    }

    /**
     * Restore the in-memory representation from the stream.
     * <p>
     *
     * @exception ClassNotFoundException Thrown if the stored representation
     *                                   is serialized and a class named in
     *                                   the stream could not be found.
     *
     * @see java.io.Externalizable#readExternal
     **/
    private final void localReadExternal(ObjectInput in)
            throws IOException, ClassNotFoundException {
//        SpliceLogUtils.trace(LOG,"localReadExternal");
        // read the format id of this conglomerate.
        conglom_format_id = FormatIdUtil.readFormatIdInteger(in);
        tmpFlag = FormatIdUtil.readFormatIdInteger(in);
        int segmentid = in.readInt();
        long containerid = in.readLong();
        id = new ContainerKey(segmentid, containerid);
        // read the number of columns in the heap.
        int num_columns = in.readInt();
        // read the array of format ids.
        format_ids = ConglomerateUtil.readFormatIdArray(num_columns, in);
        this.conglom_format_id = getTypeFormatId();
        num_columns = in.readInt();
        collation_ids = ConglomerateUtil.readFormatIdArray(num_columns, in);
        num_columns = in.readInt();
        columnOrdering = ConglomerateUtil.readFormatIdArray(num_columns, in);
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        localReadExternal(in);
    }


    public void readExternalFromArray(ArrayInputStream in) throws IOException, ClassNotFoundException {
        if (LOG.isTraceEnabled())
            LOG.trace("readExternalFromArray: ");
        localReadExternal(in);
    }

    @Override
    public int getBaseMemoryUsage() {
        return ClassSize.estimateBaseFromCatalog(HBaseConglomerate.class);
    }

    @Override
    public int getContainerKeyMemoryUsage() {
        return ClassSize.estimateBaseFromCatalog(ContainerKey.class);
    }

}
