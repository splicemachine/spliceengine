package com.splicemachine.derby.impl.sql.depend;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.Dependency;
import org.apache.derby.iapi.sql.depend.Dependent;
import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.depend.ProviderInfo;
import org.apache.derby.iapi.sql.depend.ProviderList;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.impl.sql.depend.BasicDependencyManager;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

public class SpliceDependencyManager extends BasicDependencyManager {
    private static final Logger LOG = Logger.getLogger(SpliceDependencyManager.class);
    
	public SpliceDependencyManager(DataDictionary dd) {
		super(dd);
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "initialize dependencyManager");
	}

	@Override
	public void addDependency(Dependent d, Provider p, ContextManager cm) throws StandardException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "addDependency dependent=%s,provider=%s,contextManager=%s",d,p,cm);
		super.addDependency(d, p, cm);
	}

	@Override
	public void invalidateFor(Provider p, int action, LanguageConnectionContext lcc) throws StandardException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "invalidateFor provider=%s,action=%d,lcc=%s",p,action,lcc);
		super.invalidateFor(p, action, lcc);
	}

	@Override
	public void clearDependencies(LanguageConnectionContext lcc, Dependent d) throws StandardException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "clearDependencies lcc=%s,dependent=%s",lcc,d);
		super.clearDependencies(lcc, d);
	}

	@Override
	public void clearDependencies(LanguageConnectionContext lcc, Dependent d,TransactionController tc) throws StandardException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "clearDependencies lcc=%s,dependent=%s,transactionController=%s",lcc,d,tc);
		super.clearDependencies(lcc, d, tc);
	}

	@Override
	public synchronized void clearInMemoryDependency(Dependency dy) {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "clearInMemoryDependency dependency=%s",dy);
		super.clearInMemoryDependency(dy);
	}

	@Override
	public ProviderInfo[] getPersistentProviderInfos(Dependent dependent) throws StandardException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "getPersistentProviderInfos dependent=%s",dependent);
		return super.getPersistentProviderInfos(dependent);
	}

	@Override
	public ProviderInfo[] getPersistentProviderInfos(ProviderList pl) throws StandardException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "getPersistentProviderInfos providerList=%s",pl);
		return super.getPersistentProviderInfos(pl);
	}

	@Override
	public void clearColumnInfoInProviders(ProviderList pl) throws StandardException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "clearColumnInfoInProviders providerList=%s",pl);
		super.clearColumnInfoInProviders(pl);
	}

	@Override
	public void copyDependencies(Dependent copyFrom, Dependent copyTo, boolean persistentOnly, ContextManager cm) throws StandardException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "copyDependencies copyFrom=%s,copyTo=%s,persistentOnly=%s, contextManager=%s",copyFrom,copyTo,persistentOnly,cm);
		super.copyDependencies(copyFrom, copyTo, persistentOnly, cm);
	}

	@Override
	public void copyDependencies(Dependent copyFrom, Dependent copyTo, boolean persistentOnly, ContextManager cm, TransactionController tc) throws StandardException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "copyDependencies copyFrom=%s,copyTo=%s,persistentOnly=%s, contextManager=%s, transactionController=%s",copyFrom,copyTo,persistentOnly,cm,tc);
		super.copyDependencies(copyFrom, copyTo, persistentOnly, cm, tc);
	}

	@Override
	public String getActionString(int action) {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "getActionString %d",action);
		return super.getActionString(action);
	}

	@Override
	public int countDependencies() throws StandardException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "countDependencies");
		return super.countDependencies();
	}
	
}
