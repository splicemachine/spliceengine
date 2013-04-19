package com.splicemachine.derby.impl.db;

import java.util.Properties;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.property.PropertyFactory;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.impl.db.BasicDatabase;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.log4j.Logger;

import com.splicemachine.derby.impl.store.access.HBaseStore;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceLockSpace;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;

public class SpliceDatabase extends BasicDatabase {
	private static Logger LOG = Logger.getLogger(SpliceDatabase.class);
	public void boot(boolean create, Properties startParams) throws StandardException {
		//System.setProperty("derby.language.logQueryPlan", "true");
        System.setProperty("derby.connection.requireAuthentication","false");
	    SanityManager.DEBUG_SET("ByteCodeGenInstr");
//	    SanityManager.DEBUG_SET("DumpClassFile");
        SanityManager.DEBUG_SET("DumpParseTree");
		create = true; //  Need to figure out the create bit...
		if (SpliceUtils.created())
			create = false;
		if (create)
			SpliceLogUtils.info(LOG,"Creating the Splice Machine");
		else {  
			SpliceLogUtils.info(LOG,"Booting the Splice Machine");
		}
		super.boot(create, startParams);
	}
		@Override
		protected void bootValidation(boolean create, Properties startParams) throws StandardException {
			SpliceLogUtils.trace(LOG,"bootValidation create %s, startParams %s",create,startParams);
			pf = (PropertyFactory) Monitor.bootServiceModule(create, this,org.apache.derby.iapi.reference.Module.PropertyFactory, startParams);
		}

		protected void bootStore(boolean create, Properties startParams) throws StandardException {
			SpliceLogUtils.trace(LOG,"bootStore create %s, startParams %s",create,startParams);
			af = (AccessFactory) Monitor.bootServiceModule(create, this, AccessFactory.MODULE, startParams);
		}
		
		public LanguageConnectionContext generateLanguageConnectionContext(String transactionID, ContextManager cm, String user, String drdaID, String dbname) throws StandardException {
			TransactionController tc = ((SpliceAccessManager) af).marshallTransaction(cm, transactionID);
			cm.setLocaleFinder(this);
			pushDbContext(cm);
			LanguageConnectionContext lctx = lcf.newLanguageConnectionContext(cm, tc, lf, this, user, drdaID, dbname);
			pushClassFactoryContext(cm, lcf.getClassFactory());
			ExecutionFactory ef = lcf.getExecutionFactory();
			ef.newExecutionContext(cm);
			lctx.initialize();		
			return lctx;
		}
		
		
}
