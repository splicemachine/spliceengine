package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.primitives.Bytes;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @author Scott Fines
 *         Date: 12/30/15
 */
public class OperationConfiguration{

    public static final String SEQUENCE_TABLE_NAME = "SPLICE_SEQUENCES";
    @SuppressFBWarnings(value = "MS_MUTABLE_ARRAY",justification = "Intentional")
    public static final byte[] SEQUENCE_TABLE_NAME_BYTES = Bytes.toBytes(SEQUENCE_TABLE_NAME);

    private OperationConfiguration(){}


    /**
     * The number of sequential entries to reserve in a single sequential block.
     *
     * Splice uses weakly-ordered sequential generation, in that each RegionServer will perform
     * one network operation to "reserve" a block of adjacent numbers, then it will sequentially
     * use those numbers until the block is exhausted, before fetching another block. The result
     * of which is that two different RegionServers operating concurrently with the same sequence
     * will see blocks out of order, but numbers ordered within those blocks.
     *
     * This setting configures how large those blocks may be. Turning it up will result in fewer
     * network operations during large-scale sequential id generation, and also less block-reordering
     * due to the weak-ordering. However, it will also result in a greater number of "missing" ids, since
     * a block, once allocated, can never be allocated again.
     *
     * Defaults to 1000
     */
    public static final String SEQUENCE_BLOCK_SIZE = "splice.sequence.allocationBlockSize";
    private static final int DEFAULT_SEQUENCE_BLOCK_SIZE = 1000;

    public static final SConfiguration.Defaults defaults = new SConfiguration.Defaults(){

        @Override
        public boolean hasLongDefault(String key){
            return false;
        }

        @Override
        public long defaultLongFor(String key){
            throw new IllegalArgumentException("No long default for key '"+key+"'");
        }

        @Override
        public boolean hasIntDefault(String key){
            switch(key){
                case SEQUENCE_BLOCK_SIZE: return true;
                default: return false;
            }
        }

        @Override
        public int defaultIntFor(String key){
            switch(key){
                case SEQUENCE_BLOCK_SIZE: return DEFAULT_SEQUENCE_BLOCK_SIZE;
                default:
                    throw new IllegalArgumentException("No int default for key '"+key+"'");
            }
        }

        @Override
        public boolean hasStringDefault(String key){
            return false;
        }

        @Override
        public String defaultStringFor(String key){
            throw new IllegalArgumentException("No String default for key '"+key+"'");
        }

        @Override
        public boolean defaultBooleanFor(String key){
            throw new IllegalArgumentException("No Boolean default for key '"+key+"'");
        }

        @Override
        public boolean hasBooleanDefault(String key){
            return false;
        }

        @Override
        public double defaultDoubleFor(String key){
            throw new IllegalArgumentException("No Double default for key '"+key+"'");
        }

        @Override
        public boolean hasDoubleDefault(String key){
            return false;
        }
    };
}
