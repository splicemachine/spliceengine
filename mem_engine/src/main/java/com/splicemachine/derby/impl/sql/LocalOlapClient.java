package com.splicemachine.derby.impl.sql;

import com.splicemachine.EngineDriver;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapClient;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import com.splicemachine.si.impl.driver.SIDriver;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
public class LocalOlapClient implements OlapClient{
    private static LocalOlapClient ourInstance=new LocalOlapClient();

    public static LocalOlapClient getInstance(){
        return ourInstance;
    }

    private LocalOlapClient(){ }


    @Override
    public <R extends OlapResult> R execute(@Nonnull DistributedJob jobRequest) throws IOException, TimeoutException{
        Status status=new Status();
        Callable<Void> callable=jobRequest.toCallable(status,SIDriver.driver().getClock(),Long.MAX_VALUE);
        try {
            callable.call();
        } catch (Exception e) {
            throw new IOException(e);
        }
        return (R)status.getResult();
    }

    @Override public void shutdown(){ }

    /* ****************************************************************************************************************/
    /*private helper stuff*/
    private static class Status implements OlapStatus{
        private OlapResult result;

        @Override
        public State checkState(){
            return result==null? State.RUNNING:State.COMPLETE;
        }

        @Override
        public OlapResult getResult(){
            return result;
        }

        @Override
        public void cancel(){

        }

        @Override
        public boolean isAvailable(){
            return true;
        }

        @Override
        public boolean markSubmitted(){
            return true;
        }

        @Override
        public void markCompleted(OlapResult result){
            this.result = result;
        }

        @Override
        public boolean markRunning(){
            return true;
        }

        @Override
        public boolean isRunning(){
            return result==null;
        }
    }
}
