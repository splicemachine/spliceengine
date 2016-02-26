package com.splicemachine.access;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.util.ChainedDefaults;
import com.splicemachine.constants.SpliceConfiguration;
import com.splicemachine.si.api.SIConfigurations;
import com.splicemachine.storage.StorageConfiguration;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class HConfiguration implements SConfiguration{

    public static final HConfiguration INSTANCE = new HConfiguration(SpliceConfiguration.create());

    public static final String NAMESPACE = "splice.namespace";
    public static final String DEFAULT_NAMESPACE = "splice";

    /**
     * Path in Zookeeper for storing ongoing backup
     */
    public static final String BACKUP_PATH = "splice.backup_node";
    public static final String DEFAULT_BACKUP_PATH = "/backup";
    public static final byte[] BACKUP_IN_PROGRESS = Bytes.toBytes(false);
    public static final byte[] BACKUP_DONE = Bytes.toBytes(true);

    /**
     * The Path in zookeeper for manipulating transactional information.
     * Set to [SpliceRootPath]/transactions
     */
    public static final String TRANSACTION_PATH = "/transactions";

    /**
     * The Path in zookeeper for storing the minimum active transaction.
     * Defaults to [TRANSACTION_PATH]/minimum
     */
    public static final String MINIMUM_ACTIVE_PATH = TRANSACTION_PATH+"/minimum";

    /**
     * Path in ZooKeeper for manipulating Conglomerate information.
     * Defaults to /conglomerates
     */
    public static final String CONGLOMERATE_SCHEMA_PATH = "/conglomerates";

    /**
     * Path in ZooKeeper for storing Derby properties information.
     * Defaults to /derbyPropertyPath
     */
    public static final String DERBY_PROPERTY_PATH = "/derbyPropertyPath";

    public static final String DDL_PATH="/ddl";
    public static final String DDL_CHANGE_PATH="/ddlChange";

    /**
     * Location of Startup node in ZooKeeper. The presence of this node
     * indicates whether or not Splice needs to attempt to recreate
     * System tables (i.e. whether or not Splice has been installed and
     * set up correctly).
     * Defaults to /startupPath
     */
    public static final String STARTUP_PATH = "/startupPath";

    /**
     * Location of Leader Election path in ZooKeeper.
     * Defaults to /leaderElection
     */
    public static final String LEADER_ELECTION = "/leaderElection";

    public static final String SPLICE_ROOT_PATH = "splice.root.path";
    private static final String DEFAULT_ROOT_PATH="/splice";

    /**
     * The Path in zookeeper for storing the maximum reserved timestamp
     * from the ZkTimestampSource implementation.
     * Defaults to /transactions/maxReservedTimestamp
     */
    public static final String MAX_RESERVED_TIMESTAMP_PATH = "/transactions/maxReservedTimestamp";
    public static final List<String> zookeeperPaths =Collections.unmodifiableList(Arrays.asList(
            CONGLOMERATE_SCHEMA_PATH,
            CONGLOMERATE_SCHEMA_PATH+"/__CONGLOM_SEQUENCE",
            DERBY_PROPERTY_PATH,
            CONGLOMERATE_SCHEMA_PATH,
            CONGLOMERATE_SCHEMA_PATH,
            MINIMUM_ACTIVE_PATH,
            TRANSACTION_PATH,
            MAX_RESERVED_TIMESTAMP_PATH,
            DDL_CHANGE_PATH,
            DDL_PATH
    ));

    // Splice Internal Tables
    public static final String TEST_TABLE = "SPLICE_TEST";
    public static final String TRANSACTION_TABLE = "SPLICE_TXN";
    public static final String TENTATIVE_TABLE = "TENTATIVE_DDL";
    public static final String SYSSCHEMAS_CACHE = "SYSSCHEMAS_CACHE";
    public static final String SYSSCHEMAS_INDEX1_ID_CACHE = "SYSSCHEMAS_INDEX1_ID_CACHE";
    public static final String SEQUENCE_TABLE_NAME = "SPLICE_SEQUENCES";
    @SuppressFBWarnings(value = "MS_MUTABLE_ARRAY",justification = "Intentional")
    public static final byte[] TRANSACTION_TABLE_BYTES =Bytes.toBytes(TRANSACTION_TABLE);

    /**
     * The type of compression to use when compressing Splice Tables. This is set the same way
     * HBase sets table compression, and has the same codecs available to it (GZIP,Snappy, or
     * LZO depending on what is installed).
     *
     * Defaults to none
     */
    public static final String COMPRESSION_ALGORITHM = "splice.compression";
    private static final String DEFAULT_COMPRESSION = "none";

    // Splice Default Table Definitions
    public static final Boolean DEFAULT_IN_MEMORY = HColumnDescriptor.DEFAULT_IN_MEMORY;
    public static final Boolean DEFAULT_BLOCKCACHE=HColumnDescriptor.DEFAULT_BLOCKCACHE;
    public static final int DEFAULT_TTL = HColumnDescriptor.DEFAULT_TTL;
    public static final String DEFAULT_BLOOMFILTER = HColumnDescriptor.DEFAULT_BLOOMFILTER;

//    private static final String DEFAULT_MAX_RESERVED_TIMESTAMP_PATH = "/transactions/maxReservedTimestamp";


    /**
     * The number of timestamps to 'reserve' at a time in the Timestamp Server.
     * Defaults to 8192
     */
    public static final String TIMESTAMP_BLOCK_SIZE = "splice.timestamp_server.blocksize";
    private static final int DEFAULT_TIMESTAMP_BLOCK_SIZE = 8192;

    public static final int DEFAULT_JMX_BIND_PORT = 10102;


    public static final String REGION_LOAD_UPDATE_INTERVAL = "splice.statistics.regionLoadUpdateInterval";
    public static final long DEFAULT_REGION_LOAD_UPDATE_INTERVAL = 5;

    private final Configuration delegate;
    private final ChainedDefaults defaults;

    private HConfiguration(Configuration delegate){
        this.delegate=delegate;
        this.defaults = new ChainedDefaults();
        this.defaults.addDefaults(new Defaults(delegate,this));
    }

    @Override
    public String getString(String key){
        String value=delegate.get(key);
        if(value==null){
            if(defaults.hasStringDefault(key))
                return defaults.defaultStringFor(key);
            else if(defaults.hasIntDefault(key))
                return Integer.toString(defaults.defaultIntFor(key));
            else if(defaults.hasLongDefault(key))
                return Long.toString(defaults.defaultLongFor(key));
        }
        return value;
    }

    @Override
    public Set<String> prefixMatch(String prefix){
        Map<String, String> valByRegex=delegate.getValByRegex("^prefix.*");
        return valByRegex.keySet();
    }

    @Override
    public double getDouble(String key){
        String v = delegate.get(key);
        if(v==null){
            return defaults.defaultDoubleFor(key);
        }else return Double.parseDouble(v);
    }

    @Override
    public boolean getBoolean(String key){
        boolean defBool = defaults.defaultBooleanFor(key);
        return delegate.getBoolean(key,defBool);
    }

    @Override
    public long getLong(String key){
        return delegate.getLong(key,defaults.defaultLongFor(key));
    }

    @Override
    public int getInt(String key){
        return delegate.getInt(key,defaults.defaultIntFor(key));
    }

    public void addDefaults(SConfiguration.Defaults defaults){
        this.defaults.addDefaults(defaults);
    }

    public static class Defaults implements SConfiguration.Defaults{
        private final Configuration configuration;
        private final SConfiguration owner;

        public Defaults(Configuration configuration,SConfiguration owner){
            this.configuration=configuration;
            this.owner=owner;
        }

        @Override
        public boolean hasLongDefault(String key){
            switch(key){
                case StorageConfiguration.REGION_MAX_FILE_SIZE:
                case REGION_LOAD_UPDATE_INTERVAL:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public long defaultLongFor(String key){
            switch(key){
                case StorageConfiguration.REGION_MAX_FILE_SIZE:
                    return configuration.getLong(StorageConfiguration.REGION_MAX_FILE_SIZE,HConstants.DEFAULT_MAX_FILE_SIZE);
                case REGION_LOAD_UPDATE_INTERVAL:
                    return configuration.getLong(REGION_LOAD_UPDATE_INTERVAL,DEFAULT_REGION_LOAD_UPDATE_INTERVAL);
                default:
                    throw new IllegalArgumentException("No hbase default for key '"+key+"'");
            }
        }

        @Override
        public boolean hasIntDefault(String key){
            switch(key){
                case HConstants.REGION_SERVER_HANDLER_COUNT:
                case SIConfigurations.TRANSACTION_LOCK_STRIPES:
                case TIMESTAMP_BLOCK_SIZE:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public int defaultIntFor(String key){
            assert hasIntDefault(key): "No hbase default for key '"+key+"'";
            switch(key){
                case HConstants.REGION_SERVER_HANDLER_COUNT: return HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT;
                case SIConfigurations.TRANSACTION_LOCK_STRIPES:
                    return configuration.getInt(HConstants.REGION_SERVER_HANDLER_COUNT,HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT);
                case TIMESTAMP_BLOCK_SIZE: return DEFAULT_TIMESTAMP_BLOCK_SIZE;
                default:
                    throw new IllegalArgumentException("No Hbase default for key '"+key+"'");
            }
        }

        @Override
        public boolean hasStringDefault(String key){
            switch(key){
                case SPLICE_ROOT_PATH:
                case NAMESPACE:
                case COMPRESSION_ALGORITHM:
                case BACKUP_PATH:
                    return true;
            }
            return false;
        }

        @Override
        public String defaultStringFor(String key){
            switch(key){
                case SPLICE_ROOT_PATH: return DEFAULT_ROOT_PATH;
                case NAMESPACE: return DEFAULT_NAMESPACE;
                case COMPRESSION_ALGORITHM: return DEFAULT_COMPRESSION;
                case BACKUP_PATH: return DEFAULT_BACKUP_PATH;
                default:
                    throw new IllegalArgumentException("No Hbase default for key '"+key+"'");
            }
        }

        @Override
        public boolean defaultBooleanFor(String key){
            throw new IllegalArgumentException("No Hbase default for key '"+key+"'");
        }

        @Override
        public boolean hasBooleanDefault(String key){
            return false;
        }

        @Override
        public double defaultDoubleFor(String key){
            throw new IllegalArgumentException("No Hbase default for key '"+key+"'");
        }

        @Override
        public boolean hasDoubleDefault(String key){
            return false;
        }
    };

    public Configuration unwrapDelegate(){
        return delegate;
    }
}
