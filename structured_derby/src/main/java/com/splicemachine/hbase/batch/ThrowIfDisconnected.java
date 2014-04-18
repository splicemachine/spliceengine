package com.splicemachine.hbase.batch;
 
import com.splicemachine.derby.utils.Exceptions;
import org.apache.hadoop.hbase.ipc.RpcCallContext;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

 /**
  * @author P Trolard
  *         Date: 16/04/2014
  */
 public class ThrowIfDisconnected {
    private static throwIfDisconnected thrower;

    public interface throwIfDisconnected {
        public void invoke(RpcCallContext target, String message) throws IOException;
    }

    public static throwIfDisconnected getThrowIfDisconnected(){
        if (thrower == null) {
            try {
                // Cloudera 4.3
                final Method throwIfD = RpcCallContext.class.getMethod("throwExceptionIfCallerDisconnected",new Class[]{});
                thrower = new throwIfDisconnected() {
                    @Override
                    public void invoke(RpcCallContext target, String message) throws IOException {
                        try {
                            throwIfD.invoke(target);
                        } catch (IllegalAccessException e) {
                            throw Exceptions.getIOException(e);
                        } catch (InvocationTargetException e) {
                            throw Exceptions.getIOException(e);
                        }
                    }
                };
            } catch (NoSuchMethodException e) {
                // Clouderda 4.5
                try {
                    final Method throwIfD = RpcCallContext.class.getMethod("throwExceptionIfCallerDisconnected",
                                                                              new Class[]{String.class});
                    thrower = new throwIfDisconnected() {
                        @Override
                        public void invoke(RpcCallContext target, String message) throws IOException {
                            try {
                                throwIfD.invoke(target, message);
                            } catch (IllegalAccessException e) {
                                throw Exceptions.getIOException(e);
                            } catch (InvocationTargetException e) {
                                throw Exceptions.getIOException(e);
                            }
                        }
                    };
                } catch (NoSuchMethodException e2){
                    throw new RuntimeException("Unable to resolve throwExceptinoIfCallerDisconnected" +
                                                   " method", e2);
                }
            }
        }
        return thrower;
    }

 }
