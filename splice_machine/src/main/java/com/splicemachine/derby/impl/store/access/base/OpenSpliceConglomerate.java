package com.splicemachine.derby.impl.store.access.base;

import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransactionView;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.DynamicCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.RowUtil;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.LockingPolicy;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;

import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.btree.IndexConglomerate;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;

/**
*
* This class maintains the key session items for a conglomerate.  This is usually passed into the Controllor (inserts/updates/deletes)
* or Scan Manager (Result Sets).
*
**/

public class OpenSpliceConglomerate  {
	protected SpliceConglomerate conglomerate;
	protected TransactionManager transactionManager;
	protected Transaction transaction;
	protected int openMode;
	protected int lockLevel;
	protected LockingPolicy lockingPolicy;
	protected StaticCompiledOpenConglomInfo staticCompiledOpenConglomInfo;
	protected DynamicCompiledOpenConglomInfo dynamicCompiledOpenConglomInfo;
	protected boolean hold;	
	protected DataValueDescriptor[] rowTemplate = null;
	
	public OpenSpliceConglomerate(TransactionManager transactionManager,
                                  Transaction transaction,
                                  boolean hold,
                                  int openMode,
                                  int lockLevel,
                                  LockingPolicy lockingPolicy,
                                  StaticCompiledOpenConglomInfo staticCompiledOpenConglomInfo,
                                  DynamicCompiledOpenConglomInfo dynamicCompiledOpenConglomInfo,
                                  SpliceConglomerate conglomerate) {
		this.transactionManager = transactionManager;
		this.transaction = transaction;
		try {
			((BaseSpliceTransaction)transaction).setActiveState(false, false, null);
		} catch (Exception e) {
        throw new RuntimeException(e);
		}
		this.hold = hold;
		this.lockLevel = lockLevel;
		this.lockingPolicy = lockingPolicy;
		this.staticCompiledOpenConglomInfo = staticCompiledOpenConglomInfo;
		this.dynamicCompiledOpenConglomInfo = dynamicCompiledOpenConglomInfo;
		this.openMode = openMode;
		this.conglomerate = conglomerate;
	}
    
    public int[] getFormatIds() {
        return conglomerate.getFormat_ids();
    }

    public int[] getColumnOrdering() {
        return conglomerate.getColumnOrdering();
    }


    public int[] getCollationIds() {
        return conglomerate.getCollation_ids();
    }

    public boolean[] getAscDescInfo() {
    	return conglomerate.getAscDescInfo();
    }
    
    public long getContainerID() {
    	return conglomerate.getContainerid();
    }
    
    
    /**
     * Return an "empty" row location object of the correct type.
     * <p>
     *
	 * @return The empty Rowlocation.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public RowLocation newRowLocationTemplate() throws StandardException {
		return new HBaseRowLocation();
	}
	public Conglomerate getConglomerate() {
		return this.conglomerate;
	}
	public TransactionManager getTransactionManager() {
		return transactionManager;
	}

	public Transaction getTransaction() {
		return transaction;
	}

	public int getOpenMode() {
		return openMode;
	}

	public int getLockLevel() {
		return lockLevel;
	}

	public LockingPolicy getLockingPolicy() {
		return lockingPolicy;
	}

	public StaticCompiledOpenConglomInfo getStaticCompiledOpenConglomInfo() {
		return staticCompiledOpenConglomInfo;
	}

	public DynamicCompiledOpenConglomInfo getDynamicCompiledOpenConglomInfo() {
		return dynamicCompiledOpenConglomInfo;
	}

	public boolean isHold() {
		return hold;
	}

	public DataValueDescriptor[] cloneRowTemplate() throws StandardException {
		if (rowTemplate == null)
			rowTemplate = RowUtil.newTemplate(getTransaction().getDataValueFactory(), null, getFormatIds(), getCollationIds());
		return(RowUtil.newRowFromTemplate(rowTemplate));
	}
	
	public long getIndexConglomerate() {
		return ((IndexConglomerate)this.conglomerate).baseConglomerateId;
	}

	@Override
	public String toString() {
		try {
			return String.format("OpenSpliceConglomerate {conglomerate=%s, rowTemplate=%s}",conglomerate,cloneRowTemplate());
		} catch (StandardException e) {
			e.printStackTrace();
			return String.format("OpenSpliceConglomerate {conglomerate=%s}",conglomerate);
		}
	}
	
	
	
}
