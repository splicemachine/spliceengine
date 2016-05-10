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
