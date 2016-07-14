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

