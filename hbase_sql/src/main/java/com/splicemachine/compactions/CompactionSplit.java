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

package com.splicemachine.compactions;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by jyuan on 3/24/16.
 */
public class CompactionSplit extends InputSplit implements Writable {

    String[] locations;

    public CompactionSplit() {}

    public CompactionSplit(String location) {
        locations = new String[1];
        locations[0] = location;
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return locations;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(locations.length);
        for (int i = 0; i < locations.length; ++i) {
            out.writeUTF(locations[i]);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int n = in.readInt();
        locations = new String[n];
        for (int i = 0; i < n; ++i) {
            locations[i] = in.readUTF();
        }
    }
}
