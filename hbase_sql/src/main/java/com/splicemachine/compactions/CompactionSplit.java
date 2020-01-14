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
