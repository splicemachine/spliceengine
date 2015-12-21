package com.splicemachine.si.testsetup;

import com.splicemachine.timestamp.api.TimestampSource;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class ConcurrentTimestampSource implements TimestampSource{
     private AtomicLong id =new AtomicLong(0l);
     private volatile long memory = 0;


     @Override
     public long nextTimestamp() {
          return id.incrementAndGet();
     }

     @Override
     public void rememberTimestamp(long timestamp) {
          memory = timestamp;
     }

     @Override
     public long retrieveTimestamp() {
          return memory;
     }

     @Override
     public void shutdown() {

     }
}
