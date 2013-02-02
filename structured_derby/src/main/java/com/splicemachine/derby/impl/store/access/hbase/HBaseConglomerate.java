package com.splicemachine.derby.impl.store.access.hbase;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;

import com.splicemachine.derby.utils.ConglomerateUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.cache.ClassSize;
import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowLocationRetRowSource;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.StoreCostController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.access.conglomerate.ScanManager;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.store.access.conglomerate.ConglomerateUtil;
import org.apache.derby.impl.store.access.conglomerate.OpenConglomerateScratchSpace;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;

import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceScan;
import com.splicemachine.derby.utils.SpliceUtils;

/**
 * A hbase object corresponds to an instance of a hbase conglomerate.  
 * 
 *
**/

public class HBaseConglomerate extends SpliceConglomerate {
	protected static Logger LOG = Logger.getLogger(HBaseConglomerate.class);

	public HBaseConglomerate() {
    	super();
    	if (LOG.isTraceEnabled())
    		LOG.trace("instantiate");

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
        try {
					ConglomerateUtils.createConglomerate(containerId,this);
        } catch (Exception e) {
        	LOG.error(e.getMessage(), e);
        }
        this.getContainerid();
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
	public void addColumn(TransactionManager xact_manager, int column_id,
			Storable template_column, int collation_id) throws StandardException
    {
    	if (LOG.isTraceEnabled())
    		LOG.trace("addColumn column_id=" + column_id + ", template_column=" + template_column+", table name="+getContainerid());
    	
    	HTableInterface htable = null;
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
            ConglomerateUtils.updateConglomerate(this);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw StandardException.newException("Exception closing HTable in add column",e);
		} finally {
			try{
				if (htable != null)
					htable.close();
			} catch (Exception e) {
				//no need to catch. htable is already closed or null
			}
		}
    }


	/**
	Drop this hbase conglomerate (what's the relationship with dropping container).
	@see Conglomerate#drop

	@exception StandardException Standard exception policy.
	**/
	public void drop(TransactionManager xact_manager)
		throws StandardException
	{
    	if (LOG.isTraceEnabled())
    		LOG.trace("drop " + xact_manager);

        //xact_manager.getRawStoreXact().dropContainer(id);
		//FIXME: need a new API on RawTransaction
		//xact_manager.getRawStoreXact().dropHTable(Long.toString(id.getContainerId()));
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
     * Is this conglomerate temporary?
     * <p>
     *
	 * @return whether conglomerate is temporary or not.
     **/
    public boolean isTemporary()
    {
    	if (LOG.isTraceEnabled())
    		LOG.trace("isTemporary ");
        return false;
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
		 throws StandardException
	{
    	if (LOG.isTraceEnabled())
    		LOG.trace("load rowSource" + rowSource);
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
    DynamicCompiledOpenConglomInfo  dynamic_info)
		throws StandardException
	{
    	if (LOG.isTraceEnabled())
    		LOG.trace("open conglomerate id: " + Long.toString(id.getContainerId()));
        OpenSpliceConglomerate open_conglom = new OpenSpliceConglomerate(xact_manager,rawtran,hold,open_mode,
        		lock_level,locking_policy,static_info,dynamic_info,this);
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
    	if (LOG.isTraceEnabled())
    		LOG.trace("open scan: " + Long.toString(id.getContainerId()));
		if (!RowUtil.isRowEmpty(startKeyValue) || !RowUtil.isRowEmpty(stopKeyValue))
			throw StandardException.newException(SQLState.HEAP_UNIMPLEMENTED_FEATURE);
        OpenSpliceConglomerate open_conglom = new OpenSpliceConglomerate(xact_manager,rawtran,hold,open_mode,
        		lock_level,locking_policy, static_info, dynamic_info,this);
		SpliceScan hbasescan = new SpliceScan(open_conglom,scanColumnList,startKeyValue,startSearchOperator,
				qualifier,stopKeyValue,stopSearchOperator,rawtran,false);
		return(hbasescan);
	}

	public void purgeConglomerate(TransactionManager xact_manager,Transaction rawtran) throws StandardException {
    	if (LOG.isTraceEnabled())
    		LOG.trace("purgeConglomerate: " + Long.toString(id.getContainerId()));
    }

	public void compressConglomerate(TransactionManager xact_manager,Transaction rawtran) throws StandardException {
    	if (LOG.isTraceEnabled())
    		LOG.trace("compressConglomerate: " + Long.toString(id.getContainerId()));
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
    int                             isolation_level)
		throws StandardException
	{
    	if (LOG.isTraceEnabled())
    		LOG.trace("defragmentConglomerate: " + Long.toString(id.getContainerId()));

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
    public StoreCostController openStoreCost(
    TransactionManager  xact_manager,
    Transaction         rawtran)
		throws StandardException
    {
    	if (LOG.isTraceEnabled())
    		LOG.trace("openStoreCost: " + Long.toString(id.getContainerId()));
    	OpenSpliceConglomerate open_conglom = new OpenSpliceConglomerate(xact_manager,rawtran,false,ContainerHandle.MODE_READONLY,TransactionController.MODE_TABLE,(LockingPolicy) null,(StaticCompiledOpenConglomInfo) null,(DynamicCompiledOpenConglomInfo) null, this);
        HBaseCostController hbasecost = new HBaseCostController(open_conglom);

		return(hbasecost);
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
    public DataValueDescriptor getConglom()
    {
    	if (LOG.isTraceEnabled())
    		LOG.trace("getConglom: " + Long.toString(id.getContainerId()));
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
     * @see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
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
	public void writeExternal(ObjectOutput out) throws IOException
    {
    	if (LOG.isTraceEnabled())
    		LOG.trace("writeExternal: " + Long.toString(id.getContainerId()));
        FormatIdUtil.writeFormatIdInteger(out, this.getTypeFormatId());
		out.writeInt((int) id.getSegmentId());
        out.writeLong(id.getContainerId());
        out.writeInt(format_ids.length);
        ConglomerateUtil.writeFormatIdArray(format_ids, out);  

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
		throws IOException, ClassNotFoundException
	{
    	if (LOG.isTraceEnabled())
    		LOG.trace("localReadExternal: ");
        // read the format id of this conglomerate.
        FormatIdUtil.readFormatIdInteger(in);
		int segmentid = in.readInt();
        long containerid = in.readLong();
		id = new ContainerKey(segmentid, containerid);
        // read the number of columns in the heap.
        int num_columns = in.readInt();
        // read the array of format ids.
        format_ids = ConglomerateUtil.readFormatIdArray(num_columns, in);
        this.conglom_format_id = getTypeFormatId();

    }

	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    	if (LOG.isTraceEnabled())
    		LOG.trace("readExternal: ");
        localReadExternal(in);
    }

	public void readExternalFromArray(ArrayInputStream in)
		throws IOException, ClassNotFoundException
	{
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
