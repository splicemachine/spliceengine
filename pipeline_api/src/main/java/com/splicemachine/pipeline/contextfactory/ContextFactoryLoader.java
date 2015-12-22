package com.splicemachine.pipeline.contextfactory;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.si.api.txn.TxnView;

import java.io.IOException;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public interface ContextFactoryLoader{

    void load(TxnView txn) throws IOException, InterruptedException;

    WriteFactoryGroup getForeignKeyFactories();

    WriteFactoryGroup getIndexFactories();

    WriteFactoryGroup getDDLFactories();

    Set<ConstraintFactory> getConstraintFactories();

    void ddlChange(DDLMessage.DDLChange ddlChange);
}
