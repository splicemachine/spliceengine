
package com.splicemachine.derby.impl.store.access.btree;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.util.Properties;

import com.splicemachine.derby.utils.ConglomerateUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.impl.store.access.conglomerate.OpenConglomerateScratchSpace;
import org.apache.derby.iapi.store.access.conglomerate.ScanManager;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowLocationRetRowSource;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.StoreCostController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.services.cache.ClassSize;
import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceScan;
import com.splicemachine.derby.utils.SpliceUtils;

import org.apache.derby.impl.store.access.conglomerate.ConglomerateUtil;
import org.apache.log4j.Logger;

/**
 * An index object corresponds to an instance of a btree conglomerate.  
 *
**/



public class IndexConglomerate extends SpliceConglomerate {
	protected static Logger LOG = Logger.getLogger(IndexConglomerate.class);
	protected static final String PROPERTY_BASECONGLOMID = "baseConglomerateId";
	protected static final String PROPERTY_ROWLOCCOLUMN  = "rowLocationColumn";
    public static final int FORMAT_NUMBER = StoredFormatIds.ACCESS_B2I_V5_ID;
	protected long baseConglomerateId;
	protected int rowLocationColumn;
	protected boolean[]	ascDescInfo;
    protected static int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog(IndexConglomerate.class);
    protected static int CONTAINER_KEY_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog(ContainerKey.class);
    protected static final String PROPERTY_ALLOWDUPLICATES = "allowDuplicates";
    protected static final String PROPERTY_NKEYFIELDS      = "nKeyFields";
    protected static final String PROPERTY_NUNIQUECOLUMNS  = "nUniqueColumns";
    protected static final String PROPERTY_PARENTLINKS     = "maintainParentLinks";
    public static final String PROPERTY_UNIQUE_WITH_DUPLICATE_NULLS = "uniqueWithDuplicateNulls";
	protected int nKeyFields;
	protected int nUniqueColumns;
	protected boolean allowDuplicates;
	protected boolean maintainParentLinks;
	protected boolean uniqueWithDuplicateNulls = false;

    
    public IndexConglomerate() {
    	super();
//    	if (LOG.isTraceEnabled())
//    		LOG.trace("instantiate");
    }

    @Override
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
//    	if (LOG.isTraceEnabled()) {
//    		LOG.trace("Create index ");
//    	}
    	super.create(rawtran, segmentId, input_containerid, template, columnOrder, collationIds, properties, conglom_format_id, tmpFlag);
    	if (properties == null)
    		throw(StandardException.newException(SQLState.BTREE_PROPERTY_NOT_FOUND, PROPERTY_BASECONGLOMID));
    	String property_value = properties.getProperty(PROPERTY_BASECONGLOMID);
    	if (property_value == null)
    		throw(StandardException.newException(SQLState.BTREE_PROPERTY_NOT_FOUND, PROPERTY_BASECONGLOMID));
    	baseConglomerateId = Long.parseLong(property_value);
    	property_value = properties.getProperty(PROPERTY_ROWLOCCOLUMN);
    	if (SanityManager.DEBUG) {
    		if (property_value == null)
    	            	SanityManager.THROWASSERT(PROPERTY_ROWLOCCOLUMN + "property not passed to B2I.create()");
    	}
    	rowLocationColumn = Integer.parseInt(property_value);
    	// Currently the row location column must be the last column (makes) comparing the columns in the index easier.
    	if (SanityManager.DEBUG) {
    		SanityManager.ASSERT(rowLocationColumn == template.length - 1, "rowLocationColumn is not the last column in the index");
    	    SanityManager.ASSERT(template[rowLocationColumn] instanceof RowLocation);
    	            // There must be at least one key column
    	    if (rowLocationColumn < 1)
    	    	SanityManager.THROWASSERT("rowLocationColumn (" + rowLocationColumn +") expected to be >= 1");
    	}    			
    	ascDescInfo = new boolean[template.length];
    	for (int i=0 ; i < ascDescInfo.length; i++) {
    		if (columnOrder != null && i < columnOrder.length)
    			ascDescInfo[i] = columnOrder[i].getIsAscending();
    		else
    			ascDescInfo[i] = true;  // default values - ascending order
    	}
    	
        // Check input arguments
        allowDuplicates = (Boolean.valueOf(properties.getProperty(PROPERTY_ALLOWDUPLICATES, "false"))).booleanValue();

        property_value = properties.getProperty(PROPERTY_NKEYFIELDS);
        if (property_value == null)
            throw(StandardException.newException(SQLState.BTREE_PROPERTY_NOT_FOUND, PROPERTY_NKEYFIELDS));
        nKeyFields = Integer.parseInt(property_value);
        property_value = properties.getProperty(PROPERTY_NUNIQUECOLUMNS);
        if (property_value == null)
            throw(StandardException.newException(SQLState.BTREE_PROPERTY_NOT_FOUND, PROPERTY_NUNIQUECOLUMNS));
        nUniqueColumns = Integer.parseInt(property_value);
        
        property_value = properties.getProperty(PROPERTY_UNIQUE_WITH_DUPLICATE_NULLS, "false");
        uniqueWithDuplicateNulls = new Boolean (property_value).booleanValue();
        maintainParentLinks = (Boolean.valueOf(properties.getProperty(PROPERTY_PARENTLINKS, "true"))).booleanValue();

        if (SanityManager.DEBUG) {
			if (template.length != nKeyFields)
				SanityManager.THROWASSERT("template.length (" + template.length +") expected to equal nKeyFields (" + nKeyFields + ")");
            SanityManager.ASSERT((nUniqueColumns == nKeyFields) || (nUniqueColumns == (nKeyFields - 1)));
        }    	
        try {
        	ConglomerateUtils.createConglomerate(containerId, this,rawtran.getActiveStateTxIdString());
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
	public void addColumn(
	TransactionManager  xact_manager,
    int                 column_id,
    Storable            template_column,
    int                 collation_id)
        throws StandardException {
        throw StandardException.newException(SQLState.BTREE_UNIMPLEMENTED_FEATURE);	
    }


	/**
	Drop this hbase conglomerate (what's the relationship with dropping container).
	@see Conglomerate#drop

	@exception StandardException Standard exception policy.
	**/
	public void drop(TransactionManager xact_manager) throws StandardException{
		SpliceLogUtils.trace(LOG, "drop %s",xact_manager);
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
	RowLocationRetRowSource rowSource) throws StandardException {
		SpliceLogUtils.trace(LOG, "load rowSource %s",rowSource);
        long num_rows_loaded = 0;
        return(num_rows_loaded);
	}

    
	public ConglomerateController open(
	    TransactionManager              xact_manager,
	    Transaction                     rawtran,
	    boolean                         hold,
	    int                             open_mode,
	    int                             lock_level,
	    LockingPolicy                   locking_policy,
	    StaticCompiledOpenConglomInfo   static_info,
	   DynamicCompiledOpenConglomInfo  dynamic_info) throws StandardException {
		SpliceLogUtils.trace(LOG, "open conglomerate id: %s", id);
	        OpenSpliceConglomerate open_conglom = new OpenSpliceConglomerate(xact_manager,rawtran,hold,open_mode,lock_level,locking_policy,static_info,dynamic_info,this);
					return new IndexController(open_conglom, rawtran);
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
        OpenSpliceConglomerate open_conglom = new OpenSpliceConglomerate(xact_manager,rawtran,hold,open_mode,lock_level, locking_policy, static_info, dynamic_info,this);
		SpliceScan indexScan = new SpliceScan(open_conglom,scanColumnList,startKeyValue,startSearchOperator,qualifier,stopKeyValue,stopSearchOperator,rawtran,true);
		return(indexScan);
	}

	public void purgeConglomerate(
    TransactionManager              xact_manager,
    Transaction                     rawtran) throws StandardException {
		SpliceLogUtils.trace(LOG, "purgeConglomerate: %s", id);
    }

	public void compressConglomerate(
    TransactionManager              xact_manager,
    Transaction                     rawtran) throws StandardException {
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
		SpliceLogUtils.trace(LOG,"defragmentConglomerate: ", id);
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
		throws StandardException {
    	SpliceLogUtils.trace(LOG, "openStoreCost: %s", id);
       	OpenSpliceConglomerate open_conglom = new OpenSpliceConglomerate(xact_manager,rawtran,false,ContainerHandle.MODE_READONLY,TransactionController.MODE_TABLE,(LockingPolicy) null,(StaticCompiledOpenConglomInfo) null,(DynamicCompiledOpenConglomInfo) null, this);
        IndexCostController hbasecost = new IndexCostController();
		return(hbasecost);
    }


    /**
     * Print this hbase.
     **/
    public String toString()
    {
        return (id == null) ? "null" : id.toString();
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
    	SpliceLogUtils.trace(LOG, "getConglom: %s", id);
        return(this);
    }


    /**************************************************************************
	 * Methods of Storable (via Conglomerate)
	 * Storable interface, implies Externalizable, TypedFormat
     **************************************************************************
     */

	public int getTypeFormatId()  {
		return StoredFormatIds.ACCESS_B2I_V5_ID;
	}


    /**
     * Store the stored representation of the column value in the
     * stream.
     * <p>
     * For more detailed description of the ACCESS_B2I_V3_ID format see 
     * documentation at top of file.
     *
     * @see java.io.Externalizable#writeExternal
     **/
	public void writeExternal_v10_2(ObjectOutput out) throws IOException  {
		if (LOG.isTraceEnabled())
			LOG.trace("writeExternal_v10_2");
		btreeWriteExternal(out);
		out.writeLong(baseConglomerateId);
		out.writeInt(rowLocationColumn);

        //write the columns ascend/descend information as bits
        FormatableBitSet ascDescBits = 
            new FormatableBitSet(ascDescInfo.length);

        for (int i = 0; i < ascDescInfo.length; i++)
        {	
            if (ascDescInfo[i])
                ascDescBits.set(i);
        }
        ascDescBits.writeExternal(out);
	}

    public boolean[] getAscDescInfo() {
		return ascDescInfo;
	}

	public void setAscDescInfo(boolean[] ascDescInfo) {
		this.ascDescInfo = ascDescInfo;
	}

	/**
     * Store the stored representation of the column value in the
     * stream.
     * <p>
     * For more detailed description of the ACCESS_B2I_V3_ID and 
     * ACCESS_B2I_V4_ID formats see documentation at top of file.
     *
     * @see java.io.Externalizable#writeExternal
     **/
	public void writeExternal_v10_3(ObjectOutput out) throws IOException  {
		if (LOG.isTraceEnabled())
			LOG.trace("writeExternal_v10_3");

		// First part of ACCESS_B2I_V4_ID format is the ACCESS_B2I_V3_ID format.
        writeExternal_v10_2(out);
		if (conglom_format_id == StoredFormatIds.ACCESS_B2I_V4_ID
                || conglom_format_id == StoredFormatIds.ACCESS_B2I_V5_ID)
        {
            // Now append sparse array of collation ids
            ConglomerateUtil.writeCollationIdArray(collation_ids, out);
        }
    }


    /**
     * Store the stored representation of the column value in the
     * stream.
     * <p>
     * For more detailed description of the ACCESS_B2I_V3_ID and 
     * ACCESS_B2I_V5_ID formats see documentation at top of file.
     *
     * @see java.io.Externalizable#writeExternal
     **/
    public void writeExternal(ObjectOutput out) throws IOException {
		if (LOG.isTraceEnabled())
			LOG.trace("writeExternal");
        writeExternal_v10_3 (out);
        if (conglom_format_id == StoredFormatIds.ACCESS_B2I_V5_ID)
            out.writeBoolean (isUniqueWithDuplicateNulls());
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
	private final void localReadExternal(ObjectInput in) throws IOException, ClassNotFoundException {
//        SpliceLogUtils.trace(LOG,"localReadExternal");
		btreeReadExternal(in);
		baseConglomerateId = in.readLong();
		rowLocationColumn  = in.readInt();
        // read the column sort order info
        FormatableBitSet ascDescBits = new FormatableBitSet();
        ascDescBits.readExternal(in);
        ascDescInfo = new boolean[ascDescBits.getLength()];
        for(int i =0 ; i < ascDescBits.getLength(); i++)
            ascDescInfo[i] = ascDescBits.isSet(i);
        // In memory maintain a collation id per column in the template.
        collation_ids = new int[format_ids.length];
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(!hasCollatedTypes);
        }
        
        // initialize all the entries to COLLATION_TYPE_UCS_BASIC, 
        // and then reset as necessary.  For version ACCESS_B2I_V3_ID,
        // this is the default and no resetting is necessary.
        for (int i = 0; i < format_ids.length; i++)
            collation_ids[i] = StringDataValue.COLLATION_TYPE_UCS_BASIC;

        // initialize the unique with null setting to false, to be reset
        // below when read from disk.  For version ACCESS_B2I_V3_ID and
        // ACCESS_B2I_V4_ID, this is the default and no resetting is necessary.
        setUniqueWithDuplicateNulls(false);
		if (conglom_format_id == StoredFormatIds.ACCESS_B2I_V4_ID || conglom_format_id == StoredFormatIds.ACCESS_B2I_V5_ID) {
            // current format id, read collation info from disk
            if (SanityManager.DEBUG) {
                // length must include row location column and at least
                // one other field.
                SanityManager.ASSERT(
                    collation_ids.length >= 2, 
                    "length = " + collation_ids.length);
            }

            hasCollatedTypes =
                    ConglomerateUtil.readCollationIdArray(collation_ids, in);
        }
        else if (conglom_format_id != StoredFormatIds.ACCESS_B2I_V3_ID) {
            // Currently only V3, V4 and V5 should be possible in a Derby DB.
            // Actual work for V3 is handled by default code above, so no
            // special work is necessary.

            if (SanityManager.DEBUG)
            {
                SanityManager.THROWASSERT(
                    "Unexpected format id: " + conglom_format_id);
            }
        }
        if (conglom_format_id == StoredFormatIds.ACCESS_B2I_V5_ID) {
            setUniqueWithDuplicateNulls(in.readBoolean());
        }
	}
    
    /**
     * Set if the index is unique only for non null keys
     * 
     * @param uniqueWithDuplicateNulls true if the index will be unique only for
     *                                 non null keys
     */
    public void setUniqueWithDuplicateNulls (boolean uniqueWithDuplicateNulls) 
    {
        this.uniqueWithDuplicateNulls = uniqueWithDuplicateNulls;
    }

    /**
     * Returns if the index type is uniqueWithDuplicateNulls.
     * @return is index type is uniqueWithDuplicateNulls
     */
    public boolean isUniqueWithDuplicateNulls()
    {
        return uniqueWithDuplicateNulls;
    }

	@Override
	public int getBaseMemoryUsage() {
	    return ClassSize.estimateBaseFromCatalog(IndexConglomerate.class);
	}

	@Override
	public int getContainerKeyMemoryUsage() {
	    return ClassSize.estimateBaseFromCatalog(ContainerKey.class);
	}

	@Override
	public boolean fetchMaxOnBTree(TransactionManager xact_manager,
			Transaction rawtran, long conglomId, int open_mode, int lock_level,
			LockingPolicy locking_policy, int isolation_level,
			FormatableBitSet scanColumnList, DataValueDescriptor[] fetchRow)
			throws StandardException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		if (LOG.isTraceEnabled())
			LOG.trace("readExternal");
		localReadExternal(in);
	}

	@Override
	public void readExternalFromArray(ArrayInputStream in) throws IOException, ClassNotFoundException {
		if (LOG.isTraceEnabled())
			LOG.trace("readExternalFromArray");
	        localReadExternal(in);
	}
	
	public void btreeWriteExternal(ObjectOutput out)  throws IOException {
	        FormatIdUtil.writeFormatIdInteger(out, conglom_format_id);
			out.writeLong(id.getContainerId());
			out.writeInt((int) id.getSegmentId());
			out.writeInt((nKeyFields));
			out.writeInt((nUniqueColumns));
			out.writeBoolean((allowDuplicates));
			out.writeBoolean((maintainParentLinks));
	        ConglomerateUtil.writeFormatIdArray(format_ids, out);
		}
	
	public void btreeReadExternal(ObjectInput in) throws IOException, ClassNotFoundException {
	        conglom_format_id = FormatIdUtil.readFormatIdInteger(in);
			long containerid         = in.readLong();
			int segmentid			= in.readInt();
			nKeyFields          = in.readInt();
			nUniqueColumns      = in.readInt();
			allowDuplicates     = in.readBoolean();
			maintainParentLinks = in.readBoolean();
	        // read in the array of format id's
	        format_ids = ConglomerateUtil.readFormatIdArray(this.nKeyFields, in);
			id = new ContainerKey(segmentid, containerid);
		}
	
}
