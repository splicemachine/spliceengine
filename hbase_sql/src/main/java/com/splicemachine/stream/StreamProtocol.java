package com.splicemachine.stream;

import java.io.Serializable;

/**
 * Created by dgomezferro on 6/3/16.
 */
public class StreamProtocol implements Serializable {
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

    public static class Continue implements Serializable {
    }

    public static class Close implements Serializable {
    }

}

