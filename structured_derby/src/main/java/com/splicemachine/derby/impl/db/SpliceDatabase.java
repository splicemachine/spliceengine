package com.splicemachine.derby.impl.db;

import java.util.*;

import javax.security.auth.login.Configuration;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.ast.*;
import com.splicemachine.derby.utils.Exceptions;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.property.PropertyFactory;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.store.access.AccessFactory;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.impl.db.BasicDatabase;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.log4j.Logger;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.ZkUtils;

public class SpliceDatabase extends BasicDatabase {
	private static Logger LOG = Logger.getLogger(SpliceDatabase.class);
	public void boot(boolean create, Properties startParams) throws StandardException {
		Configuration.setConfiguration(null);
		//System.setProperty("derby.language.logQueryPlan", Boolean.toString(true));
        System.setProperty("derby.language.logStatementText", Boolean.toString(true));
        System.setProperty("derby.connection.requireAuthentication","false");
        /*
         * This value is set to ensure that result sets are not materialized into memory, because
         * they may be materialized over the wire and it won't work right. DO NOT CHANGE THIS SETTING.
         * See Bug #292 for more information.
         */
        System.setProperty("derby.language.maxMemoryPerTable",Integer.toString(-1));
	    //SanityManager.DEBUG_SET("ByteCodeGenInstr");
        if(SpliceConstants.dumpClassFile)
    	    SanityManager.DEBUG_SET("DumpClassFile");
        //SanityManager.DEBUG_SET("DumpOptimizedTree");
		try {
			create = !ZkUtils.isSpliceLoaded();
		} catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG,"isSpliceLoadedOnBoot failure", Exceptions.parseException(e));
		}
		
		if (create)
			SpliceLogUtils.info(LOG,"Creating the Splice Machine");
		else {  
			SpliceLogUtils.info(LOG,"Booting the Splice Machine");
		}
		super.boot(create, startParams);
        TransactionKeepAlive.start();
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

      public LanguageConnectionContext setupConnection(ContextManager cm, String user, String drdaID, String dbname)
		throws StandardException {

        LanguageConnectionContext lctx = super.setupConnection(cm, user, drdaID, dbname);

        List<Class<? extends ISpliceVisitor>> afterOptVisitors = new ArrayList<Class<? extends ISpliceVisitor>>();
        afterOptVisitors.add(AssignRSNVisitor.class);
        afterOptVisitors.add(JoinSelector.class);
        afterOptVisitors.add(MSJJoinConditionVisitor.class);
        afterOptVisitors.add(PlanPrinter.class);

        List<Class<? extends ISpliceVisitor>> afterBindVisitors = new ArrayList<Class<? extends ISpliceVisitor>>(1);
        afterBindVisitors.add(RepeatedPredicateVisitor.class);

        lctx.setASTVisitor(new SpliceASTWalker(Collections.EMPTY_LIST, afterBindVisitors, afterOptVisitors));

		return lctx;
	   }


		/**
		 * This is the light creation of languageConnectionContext that removes 4 rpc calls per context creation.
		 * 
		 * @param transactionID
		 * @param cm
		 * @param user
		 * @param drdaID
		 * @param dbname
		 * @param sessionUserName
		 * @param defaultSchemaDescriptor
		 * @return
		 * @throws StandardException
		 */
		public LanguageConnectionContext generateLanguageConnectionContext(String transactionID, ContextManager cm, String user, String drdaID, String dbname, String sessionUserName, SchemaDescriptor defaultSchemaDescriptor) throws StandardException {
			TransactionController tc = ((SpliceAccessManager) af).marshallTransaction(cm, transactionID);
			cm.setLocaleFinder(this);
			pushDbContext(cm);
			LanguageConnectionContext lctx = lcf.newLanguageConnectionContext(cm, tc, lf, this, user, drdaID, dbname);
			pushClassFactoryContext(cm, lcf.getClassFactory());
			ExecutionFactory ef = lcf.getExecutionFactory();
			ef.newExecutionContext(cm);
			lctx.initializeSplice(sessionUserName, defaultSchemaDescriptor);		
			return lctx;
		}
		/**
		 * This will perform a lookup of the user (index and main table) and the default schema (index and main table)
		 * 
		 * This method should only be used by start() methods in coprocessors.  Do not use for sinks or observers.  
		 * 
		 * @param transactionID
		 * @param cm
		 * @param user
		 * @param drdaID
		 * @param dbname
		 * @return
		 * @throws StandardException
		 */
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
