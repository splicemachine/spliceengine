/*
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.iapi.sql.dictionary;


/**
 * @author Scott Fines
 *         Date: 2/25/15
 */
public class PhysicalStatsDescriptor extends TupleDescriptor {
    private final String hostName;
    private final int numCores;
    private final long heapSize;
    private final int numIpcThreads;

    public PhysicalStatsDescriptor(String hostName,
                                   int numCores,
                                   long heapSize,
                                   int numIpcThreads){
        this.hostName = hostName;
        this.numCores = numCores;
        this.heapSize = heapSize;
        this.numIpcThreads = numIpcThreads;
    }

    public String getHostName() { return hostName; }
    public int getNumCores() { return numCores; }
    public long getHeapSize() { return heapSize; }
    public int getNumIpcThreads() { return numIpcThreads; }
}
