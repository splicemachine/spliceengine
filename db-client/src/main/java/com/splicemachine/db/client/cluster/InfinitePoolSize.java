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
 *
 */

package com.splicemachine.db.client.cluster;


/**
 * A Pool which allows an infinite number of open connections. Useful for testing,
 * but is otherwise not really recommended.
 *
 * In effect, this does nothing
 *
 * @author Scott Fines
 *         Date: 8/23/16
 */
public class InfinitePoolSize implements PoolSizingStrategy{
    public static PoolSizingStrategy INSTANCE = new InfinitePoolSize();

    private InfinitePoolSize(){}

    @Override public void acquirePermit(){ }
    @Override public void releasePermit(){ }
    @Override public int singleServerPoolSize(){ return Integer.MAX_VALUE; }
}
