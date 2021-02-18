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

package com.splicemachine.stream;

import java.io.*;
import java.util.UUID;

/**
 * Created by dgomezferro on 6/3/16.
 */
public class StreamProtocol implements Serializable {
    private StreamProtocol(){}
    public static class Init implements Serializable, Externalizable {
        public UUID uuid;
        public int numPartitions;
        public int partition;

        public Init(){}

        public Init(UUID uuid, int numPartitions, int partition) {
            this.uuid = uuid;
            this.numPartitions = numPartitions;
            this.partition = partition;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(uuid.getMostSignificantBits());
            out.writeLong(uuid.getLeastSignificantBits());
            out.writeInt(numPartitions);
            out.writeInt(partition);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            this.uuid = new UUID(in.readLong(),in.readLong());
            numPartitions = in.readInt();
            partition = in.readInt();
        }

        @Override
        public String toString() {
            return "Init{" +
                    "uuid=" + uuid +
                    ", numPartitions=" + numPartitions +
                    ", partition=" + partition +
                    '}';
        }
    }

    public static class Skip implements Serializable, Externalizable {
        public long limit;
        public long offset;

        public Skip() {}

        public Skip(long limit, long offset) {
            this.limit = limit;
            this.offset = offset;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(limit);
            out.writeLong(offset);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            limit = in.readLong();
            offset = in.readLong();
        }

        @Override
        public String toString() {
            return "Skip{" +
                    "limit=" + limit +
                    ", offset=" + offset +
                    '}';
        }
    }

    public static class Limit implements Serializable, Externalizable {
        public long limit;

        public Limit() {}

        public Limit(long limit) {
            this.limit = limit;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(limit);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            limit = in.readLong();
        }

        @Override
        public String toString() {
            return "Limit{" +
                    "limit=" + limit +
                    '}';
        }
    }

    public static class Skipped implements Serializable, Externalizable {
        public long skipped;

        public Skipped() {}

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeLong(skipped);
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            skipped = in.readLong();
        }

        public Skipped(long skipped) {
            this.skipped = skipped;
        }

        @Override
        public String toString() {
            return "Skipped{" +
                    "skipped=" + skipped +
                    '}';
        }
    }

    public static class Continue implements Serializable, Externalizable {
        public Continue() {}
        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        }
    }

    public static class RequestClose implements Serializable, Externalizable {
        public RequestClose() {}
        public void writeExternal(ObjectOutput out) throws IOException {
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        }
    }
    public static class ConfirmClose implements Serializable, Externalizable {
        public ConfirmClose() {}
        public void writeExternal(ObjectOutput out) throws IOException {
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        }
    }

}

