package com.splicemachine.derby.ddl;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import java.util.Collection;

/**
 * Invoke the local dependency manager.
 */
class ZookeeperDDLWatcherDependencies {

    void handleDependencies(DDLChange ddlChange, Collection<LanguageConnectionContext> connectionContextList) throws StandardException {

        ContextService contextService = ContextService.getFactory();
        ContextManager contextManager = contextService.getCurrentContextManager();
        if (contextManager == null) {
            contextManager = contextService.newContextManager();
            contextService.setCurrentContextManager(contextManager);
        }

        if (DDLChangeType.DROP_TABLE.equals(ddlChange.getChangeType())) {
            try {
                DropTableDDLChangeDesc changeDescription = (DropTableDDLChangeDesc) ddlChange.getTentativeDDLDesc();
                for (LanguageConnectionContext langContext : connectionContextList) {

                    contextManager.pushContext(langContext);

                    DataDictionary dd = langContext.getDataDictionary();
                    DependencyManager dm = dd.getDependencyManager();
                    dm.invalidateFor(changeDescription.getTableId(), DependencyManager.DROP_TABLE, langContext);

                    contextManager.popContext();

                    dd.clearCaches();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        contextService.resetCurrentContextManager(contextManager);

    }

}
