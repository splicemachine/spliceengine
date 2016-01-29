package com.splicemachine.pipeline.contextfactory;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.si.api.txn.TxnView;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class UnmanagedFactoryLoader implements ContextFactoryLoader{
    public static final UnmanagedFactoryLoader INSTANCE = new UnmanagedFactoryLoader();

    private final WriteFactoryGroup fk = new ListWriteFactoryGroup(Collections.<LocalWriteFactory>emptyList());
    private final WriteFactoryGroup ddl = new ListWriteFactoryGroup(Collections.<LocalWriteFactory>emptyList());
    private final WriteFactoryGroup indices = new ListWriteFactoryGroup(Collections.<LocalWriteFactory>emptyList());
    private final Set<ConstraintFactory> constraints = Collections.emptySet();


    private UnmanagedFactoryLoader(){}
    @Override
    public void ddlChange(DDLMessage.DDLChange ddlChange){
        //no-op
    }

    @Override
    public void load(TxnView txn) throws IOException, InterruptedException{
        //no-op
    }

    @Override
    public void close(){

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
}
