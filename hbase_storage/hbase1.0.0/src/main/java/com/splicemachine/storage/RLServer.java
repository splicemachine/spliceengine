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

import org.apache.hadoop.hbase.HRegionLocation;

import java.io.IOException;

/**
 * An implementation of PartitionServer which defers to an HRegionLocation instance. It could be wrong
 * if the region moves.
 *
 * @author Scott Fines
 *         Date: 3/4/16
 */
public class RLServer implements PartitionServer{
    private final HRegionLocation regionLocation;

    public RLServer(HRegionLocation regionLocation){
        this.regionLocation=regionLocation;
    }

    @Override
    public String getHostname(){
        return regionLocation.getHostname();
    }

    @Override
    public String getHostAndPort(){
        return regionLocation.getHostnamePort();
    }

    @Override
    public int getPort(){
        return regionLocation.getPort();
    }

    @Override
    public PartitionServerLoad getLoad() throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public long getStartupTimestamp(){
        return regionLocation.getServerName().getStartcode();
    }

    @Override
    public int compareTo(PartitionServer o){
        assert o instanceof RLServer: "Cannot compare to non RegionLocationServer";
        return regionLocation.compareTo(((RLServer)o).regionLocation);
    }

    @Override
    public boolean equals(Object o){
        if(this==o) return true;
        if(!(o instanceof RLServer)) return false;

        RLServer rlServer=(RLServer)o;

        return regionLocation.equals(rlServer.regionLocation);
    }

    @Override
    public int hashCode(){
        return regionLocation.hashCode();
    }
}
