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
    }
    public static class ConfirmClose implements Serializable {
    }

}

