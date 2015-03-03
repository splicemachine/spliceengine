package com.splicemachine.derby.jdbc;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.impl.db.SpliceDatabase;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.db.Database;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Attribute;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.db.jdbc.InternalDriver;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.Properties;

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
				this ("jdbc:derby:"+ SpliceConstants.SPLICE_DB + ";create=true", new Properties());
		}

		public SpliceTransactionResourceImpl(String url, Properties info) throws SQLException {
				SpliceLogUtils.debug(LOG, "instance with url %s and properties %s",url,info);
				csf = ContextService.getFactory(); // Singleton - Not Needed
				dbname = InternalDriver.getDatabaseName(url, info); // Singleton - Not Needed
				this.url = url; // Static
				username = IdUtil.getUserNameFromURLProps(info); // Static
				drdaID = info.getProperty(Attribute.DRDAID_ATTR, null); // Static
				cm = csf.newContextManager(); // Needed
				if(csf.getCurrentContextManager()==null)
						csf.setCurrentContextManager(cm);
				database = (SpliceDatabase) Monitor.findService(Property.DATABASE_MODULE, dbname);
				if(database==null){
						SpliceLogUtils.debug(LOG,"database has not yet been created, creating now");
						try {
								if(!Monitor.startPersistentService(dbname,info)){
										throw new IllegalArgumentException("Unable to start database!");
								}
								database = (SpliceDatabase)Monitor.findService(Property.DATABASE_MODULE,dbname);
						} catch (StandardException e) {
								SpliceLogUtils.error(LOG, e);
								e.printStackTrace();
								throw PublicAPI.wrapStandardException(e);
						}
				}
		}

		public void setDatabase(SpliceDatabase db) {
				database = db;
		}

		public void marshallTransaction(TxnView txn,SpliceObserverInstructions instructions) throws StandardException, SQLException {
				if (LOG.isDebugEnabled())
						SpliceLogUtils.debug(LOG, "marshallTransaction with txn %s",instructions.getTxn());
				lcc = database.generateLanguageConnectionContext(txn,
								cm, username, drdaID, dbname, instructions.getSessionUserName(), instructions.getDefaultSchemaDescriptor());
		}

		public void marshallTransaction(TxnView txn) throws StandardException, SQLException {
				if (LOG.isDebugEnabled())
						SpliceLogUtils.debug(LOG, "marshallTransaction with transactionID %s",txn);
				lcc = database.generateLanguageConnectionContext(txn,cm, username, drdaID, dbname);
		}


		public void close(){
				if(cm!=null){
						while(!cm.isEmpty()){
								cm.popContext();
						}
						csf.resetCurrentContextManager(cm);
				}
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

		public final void restoreContextStack() {
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

		public void cleanup() {
				cleanupOnError(StandardException.closeException(), false);
		}

		/**
		 * clean up error and print it to derby.log if diagActive is true
		 * @param e the error we want to clean up
		 * @param diagActive
		 *        true if extended diagnostics should be considered,
		 *        false not interested of extended diagnostic information
		 * @return true if the context manager is shutdown, false otherwise.
		 */
		boolean cleanupOnError(Throwable e, boolean diagActive)
		{
				if (SanityManager.DEBUG)
						SanityManager.ASSERT(cm != null, "cannot cleanup on error with null context manager");

				//DERBY-4856 thread dump
				return cm.cleanupOnError(e, diagActive);
		}

		public void resetContextManager(){
				csf.forceRemoveContext(cm);
		}

		public void prepareContextManager() {
				cm.setActiveThread();
				csf.setCurrentContextManager(cm);
		}

		public void popContextManager() {
				csf.resetCurrentContextManager(cm);
		}
}

