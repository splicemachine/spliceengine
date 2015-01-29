package com.splicemachine.derby.impl.store.access.base;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;

import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.cache.ClassSize;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.store.access.conglomerate.ConglomerateUtil;
import org.apache.derby.impl.store.access.conglomerate.GenericConglomerate;
import org.apache.log4j.Logger;


public abstract class SpliceConglomerate extends GenericConglomerate implements Conglomerate, StaticCompiledOpenConglomInfo {
    private static final long serialVersionUID = 7583841286945209190l;
	protected static Logger LOG = Logger.getLogger(SpliceConglomerate.class);
	protected int conglom_format_id;
	protected int tmpFlag;
	protected ContainerKey id;
	protected int[]    format_ids;
	protected int[]   collation_ids;
	protected int[] columnOrdering; // Primary Key Information
	protected boolean hasCollatedTypes;
	protected long nextContainerId = System.currentTimeMillis();
	protected long containerId;
	public SpliceConglomerate() {
//        SpliceLogUtils.trace(LOG,"instantiate");
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
        SpliceLogUtils.trace(LOG, "create segmentId %d, input_containerid %d", segmentId,input_containerid);
		if (properties != null) {
			String value = properties.getProperty(RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER);
			int minimumRecordSize = (value == null) ? RawStoreFactory.MINIMUM_RECORD_SIZE_DEFAULT : Integer.parseInt(value);
			if (minimumRecordSize < RawStoreFactory.MINIMUM_RECORD_SIZE_DEFAULT) {
				properties.put(RawStoreFactory.MINIMUM_RECORD_SIZE_PARAMETER,Integer.toString(RawStoreFactory.MINIMUM_RECORD_SIZE_DEFAULT));
			}
		}
		if (columnOrder != null) {
			columnOrdering = new int[columnOrder.length];
			for (int i=0;i<columnOrder.length;i++) {
				columnOrdering[i] = columnOrder[i].getColumnId();
			}
		} else {
			columnOrdering = new int[0];
		}
		containerId = input_containerid;
		id = new ContainerKey(segmentId, containerId);
		if ((template == null) || (template.length == 0)) {
			throw StandardException.newException(SQLState.HEAP_COULD_NOT_CREATE_CONGLOMERATE);
		}

		this.format_ids = ConglomerateUtil.createFormatIds(template);
		this.conglom_format_id = conglom_format_id;
		collation_ids = ConglomerateUtil.createCollationIds(format_ids.length, collationIds);
		hasCollatedTypes = hasCollatedColumns(collation_ids);
		this.tmpFlag = tmpFlag;
		
		try {
			((SpliceTransaction)rawtran).setActiveState(false, false, null);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void boot_create(long containerid,DataValueDescriptor[]   template) {
		id = new ContainerKey(0, containerid);
		this.format_ids = ConglomerateUtil.createFormatIds(template);
	}

	synchronized long getNextId() {
		if (LOG.isTraceEnabled())
			LOG.trace("getNextId ");
		return nextContainerId++;
	}

	public int estimateMemoryUsage() {
		if (LOG.isTraceEnabled())
			LOG.trace("estimate Memory Usage");
		int sz = getBaseMemoryUsage();

		if( null != id)
			sz += getContainerKeyMemoryUsage();
		if( null != format_ids)
			sz += format_ids.length*ClassSize.getIntSize();
		return sz;
	}


	public final ContainerKey getId() {
		if (LOG.isTraceEnabled())
			LOG.trace("getId ");
		return(id);
	}

	public boolean[] getAscDescInfo() {
		return null;
	}

	public final long getContainerid() {
		return(id.getContainerId());
	}

	public int[] getFormat_ids() {
		return format_ids;
	}
	public int[] getCollation_ids() {
		return collation_ids;
	}

	public boolean isNull() {
		return id == null;
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
		return (tmpFlag & TransactionController.IS_TEMPORARY) == TransactionController.IS_TEMPORARY;
	}

	public void restoreToNull() {
		id = null;
	}

	public String toString() {
		return (id == null) ? "null" : id.toString();
	}
	
	public int[] getColumnOrdering() {
		return columnOrdering;
	}
	public void setColumnOrdering(int[] columnOrdering) {
		this.columnOrdering = columnOrdering;
	}
	public abstract int getBaseMemoryUsage();
	public abstract int getContainerKeyMemoryUsage();
	public abstract void writeExternal(ObjectOutput out) throws IOException;
	public abstract void readExternal(ObjectInput in) throws IOException, ClassNotFoundException;
	public abstract int getTypeFormatId();
	
	


}
