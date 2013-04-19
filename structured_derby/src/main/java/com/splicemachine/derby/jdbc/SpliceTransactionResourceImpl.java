package com.splicemachine.derby.jdbc;

import java.sql.SQLException;
import java.util.Properties;
import org.apache.derby.iapi.db.Database;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.Attribute;
import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.jdbc.InternalDriver;
import org.apache.log4j.Logger;
import com.splicemachine.derby.impl.db.SpliceDatabase;
import com.splicemachine.utils.SpliceLogUtils;

public final class SpliceTransactionResourceImpl {
	private static final Logger LOG = Logger.getLogger(SpliceTransactionResourceImpl.class);
	protected ContextManager cm;
	protected ContextService csf;
	protected String username;
	private String dbname;
	private String url;
	private String drdaID;
	protected SpliceDatabase database;
	protected LanguageConnectionContext lcc;

	public SpliceTransactionResourceImpl() throws SQLException {
		this ("jdbc:derby:wombat;create=true", new Properties());
	}
	
	public SpliceTransactionResourceImpl(String url, Properties info) throws SQLException {
		SpliceLogUtils.debug(LOG, "instance with url %s and properties %s",url,info);
		csf = ContextService.getFactory();
		dbname = InternalDriver.getDatabaseName(url, info);
		this.url = url;
		username = IdUtil.getUserNameFromURLProps(info);
		drdaID = info.getProperty(Attribute.DRDAID_ATTR, null);
		cm = csf.newContextManager();
		ContextService.getFactory().setCurrentContextManager(cm);
		database = (SpliceDatabase) Monitor.findService(Property.DATABASE_MODULE, dbname);
	}

	public void setDatabase(SpliceDatabase db) {
		database = db;
	}

	public void marshallTransaction(String transactionID) throws StandardException, SQLException {
		SpliceLogUtils.debug(LOG, "marshallTransaction with transactionID %s",transactionID);
		lcc = database.generateLanguageConnectionContext(transactionID,cm, username, drdaID, dbname);
	}

	public ContextService getCsf() {
		return csf;
	}

	/**
	 * need to be public because it is in the XATransactionResource interface
	 */
	public ContextManager getContextManager() {
		return cm;
	}

	public LanguageConnectionContext getLcc() {
		return lcc;
	}

	public String getDBName() {
		return dbname;
	}

	public String getUrl() {
		return url;
	}

	public Database getDatabase() {
		return database;
	}

	/**
	 * local transaction demarcation - note that global or xa transaction cannot
	 * commit thru the connection, they can only commit thru the XAResource,
	 * which uses the xa_commit or xa_rollback interface as a safeguard.
	 */
	public void commit() throws StandardException {
		lcc.userCommit();
	}

	public void rollback() throws StandardException {
		if (lcc != null)
			lcc.userRollback();
	}

	void clearContextInError() {
		csf.resetCurrentContextManager(cm);
		cm = null;
	}

	void clearLcc() {
		lcc = null;
	}

	final void setupContextStack() {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(cm != null,
					"setting up null context manager stack");
		}
		csf.setCurrentContextManager(cm);
	}

	final void restoreContextStack() {
		if ((csf == null) || (cm == null))
			return;
		csf.resetCurrentContextManager(cm);
	}
	
	public String getUserName() {
		return username;
	}

	boolean isIdle() {
		return (lcc == null || lcc.getTransactionExecute().isIdle());
	}


}

