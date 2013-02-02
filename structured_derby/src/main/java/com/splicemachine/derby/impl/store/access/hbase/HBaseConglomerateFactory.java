package com.splicemachine.derby.impl.store.access.hbase;

import java.util.Properties;

import com.splicemachine.derby.utils.ConglomerateUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.access.conglomerate.ConglomerateFactory;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.log4j.Logger;

import com.splicemachine.derby.impl.store.access.base.SpliceConglomerateFactory;
import com.splicemachine.derby.utils.SpliceUtils;


/**
 *
 *
**/

public class HBaseConglomerateFactory extends SpliceConglomerateFactory {
	protected static Logger LOG = Logger.getLogger(HBaseConglomerate.class);
	public HBaseConglomerateFactory() {
		super();
	}

	@Override
	protected String getImplementationID() {
		return "heap";
	}

	@Override
	protected String getFormatUUIDString() {
		return "D2976090-D9F5-11d0-B54D-00A024BF8878";
	}
	
	@Override
    public int getConglomerateFactoryId() {
        return(ConglomerateFactory.HEAP_FACTORY_ID);
    }

	/**
	Create the conglomerate and return a conglomerate object for it.

	@exception StandardException Standard exception policy.

	@see ConglomerateFactory#createConglomerate
	**/
	public Conglomerate createConglomerate(	
    TransactionManager      xact_mgr,
    int                     segment,
    long                    input_containerid,
    DataValueDescriptor[]   template,
	ColumnOrdering[]        columnOrder,
    int[]                   collationIds,
    Properties              properties,
	int                     temporaryFlag)
		throws StandardException
	{
		HBaseConglomerate hbase = new HBaseConglomerate();

		hbase.create(
            xact_mgr.getRawStoreXact(), segment, input_containerid, 
            template, columnOrder, collationIds, properties, 
            hbase.getTypeFormatId(), 
            temporaryFlag);

		return hbase;
	}

    /**
     * Return Conglomerate object for conglomerate with container_key.
     * <p>
     * Return the Conglomerate Object.  This is implementation specific.
     * Examples of what will be done is using the key to find the file where
     * the conglomerate is located, and then executing implementation specific
     * code to instantiate an object from reading a "special" row from a
     * known location in the file.  In the btree case the btree conglomerate
     * is stored as a column in the control row on the root page.
     * <p>
     * This operation is costly so it is likely an implementation using this
     * will cache the conglomerate row in memory so that subsequent accesses
     * need not perform this operation.
     *
     * @param xact_mgr      transaction to perform the create in.
     * @param container_key The unique id of the existing conglomerate.
     *
	 * @return An instance of the conglomerate.
     *
	 * @exception  StandardException  Standard exception policy.
	 * 
	 * FIXME: need to 
     **/
    public Conglomerate readConglomerate(
    TransactionManager      xact_mgr,
    ContainerKey            container_key)
		throws StandardException
    {
    	if (LOG.isTraceEnabled()) {
    		LOG.trace("readConglomerate container_key " + container_key.getContainerId());
    	}
    	return ConglomerateUtils.readConglomerate(container_key.getContainerId(), HBaseConglomerate.class);
    }
	


}

