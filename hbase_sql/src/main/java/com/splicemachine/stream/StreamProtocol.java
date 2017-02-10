/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import java.io.Serializable;
import java.util.UUID;

/**
 * Created by dgomezferro on 6/3/16.
 */
public class StreamProtocol implements Serializable {
    private StreamProtocol(){}
    public static class Init implements Serializable {
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
        public String toString() {
            return "Init{" +
                    "uuid=" + uuid +
                    ", numPartitions=" + numPartitions +
                    ", partition=" + partition +
                    '}';
        }
    }

    public static class Skip implements Serializable {
        public long limit;
        public long offset;

        public Skip() {}

        public Skip(long limit, long offset) {
            this.limit = limit;
            this.offset = offset;
        }

        @Override
        public String toString() {
            return "Skip{" +
                    "limit=" + limit +
                    ", offset=" + offset +
                    '}';
        }
    }

    public static class Limit implements Serializable {
        public long limit;

        public Limit() {}

        public Limit(long limit) {
            this.limit = limit;
        }

        @Override
        public String toString() {
            return "Limit{" +
                    "limit=" + limit +
                    '}';
        }
    }

    public static class Skipped implements Serializable {
        public long skipped;

        public Skipped() {}

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

    public static class Continue implements Serializable {
    }

    public static class RequestClose implements Serializable {
        public RequestClose() {
        }
    }
    public static class ConfirmClose implements Serializable {
    }

}

