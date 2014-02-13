package com.splicemachine.derby.impl.db;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

public class TransactionKeepAlive {
    private static final Logger LOG = Logger.getLogger(TransactionKeepAlive.class);

    private static boolean running = false;
    private static final int intervalMS = SIConstants.transactionKeepAliveInterval;
    private static final TimerTask keepAliveTask = new TimerTask() {
        @Override
        public void run() {
            try {
                performKeepAlive();
            } catch (Throwable e) {
                // ignore this so the task will run again
                LOG.error("error running keepAlive", e);
            }
        }
    };

    public synchronized static void start() {
        if (running) {
            LOG.trace("keepAlive task already scheduled to run");
        } else {
            LOG.trace("scheduling keepAlive task");
            new Timer().scheduleAtFixedRate(keepAliveTask, intervalMS, intervalMS);
            running = true;
        }
    }

    private static void performKeepAlive() {
    	SpliceLogUtils.trace(LOG,"running keepAlive task");
        final ContextService contextService = ContextService.getFactory();
        @SuppressWarnings("rawtypes")
		final Set contextManagers = getContextManagers(contextService);
        if (contextManagers != null) {
            SpliceLogUtils.trace(LOG,"contextManager count %d",contextManagers.size());
            final Set<String> keptAlive = new HashSet<String>();
            for (Object o : contextManagers) {
                final ContextManager contextManager = (ContextManager) o;
                final String transactionId = SpliceObserverInstructions.getTransactionId(contextManager);
                if (transactionId != null && !keptAlive.contains(transactionId)) {
                    final TransactionManager transactor = HTransactorFactory.getTransactionManager();
                    try {
                    	SpliceLogUtils.trace(LOG,"keeping alive %s",transactionId);
                        transactor.keepAlive(transactor.transactionIdFromString(transactionId));
                        keptAlive.add(transactionId);
                    } catch (IOException e) {
                        // ignore this and carry on with the rest of the transactions
                        LOG.error("error calling keepAlive", e);
                    }
                }
            }
            SpliceLogUtils.trace(LOG, "keptAlive count %d",keptAlive.size());
            LOG.trace("keptAlive count " + keptAlive.size());
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
	private static Set getContextManagers(ContextService contextService) {
        try {
            final Field allContextsField = contextService.getClass().getDeclaredField("allContexts");
            allContextsField.setAccessible(true);
            synchronized (contextService) {
                final Set result = new HashSet();
                final Set allContexts = (Set) allContextsField.get(contextService);
                result.addAll(allContexts);
                return Collections.unmodifiableSet(result);
            }
        } catch (IllegalAccessException e) {
            LOG.error("error accessing allContexts field", e);
        } catch (NoSuchFieldException e) {
            LOG.error("error finding allContexts field", e);
        }
        return null;
    }
}
