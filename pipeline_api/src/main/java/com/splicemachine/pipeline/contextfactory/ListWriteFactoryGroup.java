package com.splicemachine.pipeline.contextfactory;

import com.splicemachine.pipeline.context.PipelineWriteContext;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
@ThreadSafe
public class ListWriteFactoryGroup implements WriteFactoryGroup{
    private final List<LocalWriteFactory> factories;

    public ListWriteFactoryGroup(){
        this.factories=new CopyOnWriteArrayList<>();
    }
    public ListWriteFactoryGroup(List<LocalWriteFactory> factories){
        this.factories=factories;
    }

    @Override
    public void addFactory(LocalWriteFactory writeFactory){
        factories.add(writeFactory);
    }

    @Override
    public void addFactories(PipelineWriteContext context,boolean keepState,int expectedWrites) throws IOException{
        for(LocalWriteFactory lwf:factories){
            lwf.addTo(context,keepState,expectedWrites);
        }
    }

    @Override
    public void replace(LocalWriteFactory newFactory){
        synchronized(factories){
            for(int i=0;i<factories.size();i++){
                LocalWriteFactory localWriteFactory=factories.get(i);
                if(localWriteFactory.equals(newFactory)){
                    if(localWriteFactory.canReplace(newFactory)){
                        localWriteFactory.replace(newFactory);
                    }else{
                        factories.set(i,newFactory);
                    }
                    return;
                }
            }
        }
    }

    public List<LocalWriteFactory> list(){
        return factories;
    }

    @Override
    public void clear(){
        factories.clear();
    }

    @Override
    public boolean isEmpty(){
        return factories.isEmpty();
    }
}
