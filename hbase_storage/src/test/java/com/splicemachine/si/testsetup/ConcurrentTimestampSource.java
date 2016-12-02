/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.si.testsetup;

import com.splicemachine.si.constants.SIConstants;
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
          return id.addAndGet(SIConstants.TRASANCTION_INCREMENT);
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
