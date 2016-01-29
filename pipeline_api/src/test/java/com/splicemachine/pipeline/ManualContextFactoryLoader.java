package com.splicemachine.pipeline;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.pipeline.contextfactory.*;
import com.splicemachine.si.api.txn.TxnView;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class ManualContextFactoryLoader implements ContextFactoryLoader{
    private final Set<ConstraintFactory> constraints = new CopyOnWriteArraySet<>();
    private final WriteFactoryGroup indices = new ListWriteFactoryGroup(Collections.<LocalWriteFactory>emptyList());
    private final WriteFactoryGroup fk = new ListWriteFactoryGroup(Collections.<LocalWriteFactory>emptyList());
    private final WriteFactoryGroup ddl = new ListWriteFactoryGroup(Collections.<LocalWriteFactory>emptyList());

    @Override
    public void load(TxnView txn) throws IOException, InterruptedException{
        //no-op, because we expect to manually add them
    }

    @Override
    public WriteFactoryGroup getForeignKeyFactories(){
        return fk;
    }

    @Override
    public WriteFactoryGroup getIndexFactories(){
        return indices;
    }

    @Override
    public WriteFactoryGroup getDDLFactories(){
        return ddl;
    }

    @Override
    public Set<ConstraintFactory> getConstraintFactories(){
        return constraints;
    }

    @Override
    public void ddlChange(DDLMessage.DDLChange ddlChange){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void close(){
        //no-op
    }
}
