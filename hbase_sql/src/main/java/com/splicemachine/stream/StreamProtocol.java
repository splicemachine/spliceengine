package com.splicemachine.stream;

import java.io.Serializable;

/**
 * Created by dgomezferro on 6/3/16.
 */
public class StreamProtocol implements Serializable {
    private StreamProtocol(){}
    public static class Init implements Serializable {
        public int numPartitions;
        public int partition;

        public Init(){}

        public Init(int numPartitions, int partition) {
            this.numPartitions = numPartitions;
            this.partition = partition;
        }

        @Override
        public String toString() {
            return "Init{" +
                    "numPartitions=" + numPartitions +
                    ", partition=" + partition +
                    '}';
        }
    }

    public static class Skip implements Serializable {
        public long toSkip;

        public Skip() {}

        public Skip(long toSkip) {
            this.toSkip = toSkip;
        }

        @Override
        public String toString() {
            return "Skip{" +
                    "toSkip=" + toSkip +
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

