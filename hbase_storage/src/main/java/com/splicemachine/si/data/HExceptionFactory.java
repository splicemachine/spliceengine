/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.si.data;

import com.splicemachine.access.api.OperationCancelledException;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.ReadOnlyModificationException;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.WriteConflict;
import com.splicemachine.si.api.txn.lifecycle.CannotCommitException;
import com.splicemachine.si.api.txn.lifecycle.TransactionTimeoutException;
import com.splicemachine.si.impl.*;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.exceptions.ConnectionClosingException;
import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.spark.SparkException;
import splice.com.google.common.base.Throwables;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.regex.Pattern;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class HExceptionFactory implements ExceptionFactory{
    public static final HExceptionFactory INSTANCE = new HExceptionFactory();

    protected HExceptionFactory(){}
    @Override
    public IOException writeWriteConflict(long txn1,long txn2){
        return new HWriteConflict(txn1,txn2);
    }

    @Override
    public IOException readOnlyModification(String message){
        return new HReadOnlyModificationException(message);
    }

    @Override
    public IOException noSuchFamily(String message){
        return new NoSuchColumnFamilyException(message);
    }

    @Override
    public IOException transactionTimeout(long tnxId){
        return new HTransactionTimeout(tnxId);
    }

    @Override
    public IOException cannotCommit(long txnId,Txn.State actualState){
        return new HCannotCommitException(txnId,actualState);
    }

    @Override
    public IOException cannotCommit(String message){
        return new HCannotCommitException(message);
    }

    @Override
    public IOException doNotRetry(Throwable t){
        return new DoNotRetryIOException(t);
    }

    @Override
    public boolean allowsRetry(Throwable error){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public IOException callerDisconnected(String message){
        return new HCallerDisconnected(message);
    }

    @Override
    public IOException failedServer(String message){
        return new HFailedServer(message);
    }

    @Override
    public IOException notServingPartition(String s){
        return new HNotServingRegion(s);
    }

    @Override
    public IOException connectionClosingException() { return new HNotServingRegion("Not Serving Region"); }

    @Override
    public IOException additiveWriteConflict(){
        return new AdditiveWriteConflict();
    }

    @Override
    public IOException doNotRetry(String message){
        return new DoNotRetryIOException(message);
    }

    @Override
    public IOException processRemoteException(Throwable e){
        e=Throwables.getRootCause(e);
        if(e instanceof RemoteException){
            Throwable t;
            try{
                /*
                 * unwrapRemoteException will set the RemoteException as the cause of this error,
                 * even though that's insane. This means that future calls to getRootCause() will
                 * undo what we take care of here, so we have to manually unwrap it using reflection
                 * to avoid setting the cause directly.
                 */
                Class<?> errorClazz=Class.forName(((RemoteException)e).getClassName());
                Constructor<?> cn=errorClazz.getConstructor(String.class);
                cn.setAccessible(true);
                t=(Throwable)cn.newInstance(e.getMessage());
            }catch(ClassNotFoundException
                    |NoSuchMethodException
                    |InstantiationException
                    |IllegalAccessException
                    |InvocationTargetException err){
                t=((RemoteException)e).unwrapRemoteException();
            }
            e=t;
        }
        if(e instanceof NotServingRegionException || e instanceof ConnectionClosingException){
            return new HNotServingRegion(e.getMessage());
        }else if(e instanceof RegionTooBusyException){
            return new HRegionTooBusy(e.getMessage());
        }else if(e instanceof WrongRegionException){
            return new HWrongRegion(e.getMessage());
        }else if(e instanceof WriteConflict){
            assert e instanceof IOException:"Programmer error: WriteConflict should be an IOException";
            return (IOException)e;
        }else if(e instanceof ReadOnlyModificationException){
            assert e instanceof IOException:"Programmer error: ReadOnlyModificationException should be an IOException";
            return (IOException)e;
        }else if(e instanceof TransactionTimeoutException){
            assert e instanceof IOException:"Programmer error: TransactionTimeoutException should be an IOException";
            return (IOException)e;
        }else if(e instanceof CannotCommitException){
            assert e instanceof IOException:"Programmer error: CannotCommitException should be an IOException";
            return (IOException)e;
        }else if(e instanceof RetriesExhaustedWithDetailsException){
            RetriesExhaustedWithDetailsException rewde=(RetriesExhaustedWithDetailsException)e;
            for(Throwable c : rewde.getCauses()){
                if(c instanceof IOException){
                    return processRemoteException(c);
                }
            }
            return processRemoteException(rewde.getCause(0));
        }else if(e instanceof CallTimeoutException){
            return new HCallTimeout(e.getMessage());
        }else if(e instanceof IOException) {
            /**
             * Rpc client may get an IOException with an error message containing "HRegionInfo was null"
             * when preparing rpc invocation, because a region is in a transitional state. If that happens,
             * return a retriable exception
             *
             */
            String message = e.getMessage();
            if (message != null && message.contains("HRegionInfo was null"))
                return new HRegionTooBusy(message);
            else
                return (IOException)e;
        } else if(e instanceof SparkException){
            return parseSparkException((SparkException)e);
        } else
            return new IOException(e);
    }

    private static final Pattern SPARK_CANCELLATION_PATTERN = Pattern.compile("Job [0-9]+ cancelled");
    private IOException parseSparkException(SparkException e){
        String errMessage = e.getMessage();
        if(errMessage==null)
            return new IOException("Unknown Spark exception");
        else if(SPARK_CANCELLATION_PATTERN.matcher(errMessage).find())
            return new OperationCancelledException();
        else return new IOException(e);
    }
}
