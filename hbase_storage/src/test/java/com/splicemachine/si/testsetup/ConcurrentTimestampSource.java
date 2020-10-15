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
     public long currentTimestamp() {
          return id.get();
     }

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

     @Override
     public void bumpTimestamp(long timestamp) {

     }
}
