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

package com.splicemachine.storage;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class MPartitionServer implements PartitionServer{
    @Override
    public int compareTo(PartitionServer o){
        return 0;  //all servers are the same in the in-memory version
    }

    @Override
    public String getHostname(){
        return "localhost";
    }

    @Override
    public String getHostAndPort(){
        return "localhost:1527";
    }

    @Override
    public int getPort(){
        return 1527;
    }

    @Override
    public PartitionServerLoad getLoad() throws IOException{
        return new PartitionServerLoad(){
            @Override public int numPartitions(){ return 0; }
            @Override public long totalWriteRequests(){ return 0; }
            @Override public long totalReadRequests(){ return 0; }
            @Override public long totalRequests(){ return 0; }
            @Override
            public Set<PartitionLoad> getPartitionLoads(){
                return Collections.emptySet();
            }
        };
    }

    @Override
    public long getStartupTimestamp(){
        return 0l;
    }

    @Override
    public boolean equals(Object o){
        if(o==this) return true;
        else if(!(o instanceof PartitionServer)) return false;
        else return compareTo((PartitionServer)o)==0;
    }

    @Override
    public int hashCode(){
        return 1;
    }
}
