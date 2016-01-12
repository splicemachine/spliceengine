package com.splicemachine.derby.lifecycle;

import com.splicemachine.SQLConfiguration;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.derby.impl.db.AuthenticationConfiguration;
import com.splicemachine.derby.impl.stats.StatsConfiguration;
import com.splicemachine.lifecycle.DatabaseLifecycleManager;
import com.splicemachine.pipeline.MPipelineEnv;
import com.splicemachine.pipeline.PipelineConfiguration;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.PipelineEnvironment;
import com.splicemachine.si.MemSIEnvironment;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.MPartitionFactory;
import com.splicemachine.storage.MTxnPartitionFactory;
import com.splicemachine.storage.StorageConfiguration;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class MemDatabase{

    public static void main(String...args) throws Exception{
        //load SI
        MemSIEnvironment env=new MemSIEnvironment(new MPipelinePartitionFactory(new MTxnPartitionFactory(new MPartitionFactory())));
        MemSIEnvironment.INSTANCE = env;
        SIDriver.loadDriver(env);
        final SIDriver driver = env.getSIDriver();
        SConfiguration config = driver.getConfiguration();
        config.addDefaults(StorageConfiguration.defaults);
        config.addDefaults(PipelineConfiguration.defaults);
        config.addDefaults(SQLConfiguration.defaults);
        config.addDefaults(AuthenticationConfiguration.defaults);
        config.addDefaults(StatsConfiguration.defaults);
        config.addDefaults(NATURAL_DEFAULTS);
        //start the database sequence
        EngineLifecycleService els = new EngineLifecycleService(new DistributedDerbyStartup(){
            @Override
            public void distributedStart() throws IOException{
                PartitionFactory tableFactory=driver.getTableFactory();
                final PartitionAdmin admin=tableFactory.getAdmin();
                admin.newPartition().withName("SPLICE_RESTORE").create(); //create REstore
                admin.newPartition().withName("SPLICE_TXN").create();
                admin.newPartition().withName("TENTATIVE_DDL").create();
                admin.newPartition().withName("SPLICE_CONGLOMERATE").create();
                admin.newPartition().withName("SPLICE_SEQUENCES").create();
            }

            @Override
            public void markBootFinished() throws IOException{

            }

            @Override
            public boolean connectAsFirstTime(){
                return true;
            }
        },config);
        DatabaseLifecycleManager manager=DatabaseLifecycleManager.manager();
        manager.registerEngineService(els);
        manager.registerNetworkService(new NetworkLifecycleService(config));
        manager.start();
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable(){
            @Override
            public void run(){
               DatabaseLifecycleManager.manager().shutdown();
            }
        }));

        while(true){
            Thread.sleep(10000);
        }
    }

    private static final SConfiguration.Defaults NATURAL_DEFAULTS = new SConfiguration.Defaults(){
            @Override
            public boolean hasLongDefault(String key){
                switch(key){
                    case StorageConfiguration.REGION_MAX_FILE_SIZE:
                        return true;
                    default:
                        return false;
                }
            }

            @Override
            public long defaultLongFor(String key){
                switch(key){
                    case StorageConfiguration.REGION_MAX_FILE_SIZE:
                        return Long.MAX_VALUE;
                    default:
                        throw new IllegalStateException("No long default for key '"+key+"'");
                }
            }

            @Override
            public boolean hasIntDefault(String key){
                switch(key){
                    case PipelineConfiguration.IPC_THREADS:
                    case SQLConfiguration.PARTITIONSERVER_PORT:
                        return true;
                    default:
                        return false;
                }
            }

            @Override
            public int defaultIntFor(String key){
                switch(key){
                    case PipelineConfiguration.IPC_THREADS: return 100;
                    case SQLConfiguration.PARTITIONSERVER_PORT: return 16020;
                    default:
                        throw new IllegalStateException("No int default found for key '"+key+"'");
                }
            }

            @Override
            public boolean hasStringDefault(String key){
                return false;
            }

            @Override
            public String defaultStringFor(String key){
                throw new IllegalStateException("No String default found for key '"+key+"'");
            }

            @Override
            public boolean defaultBooleanFor(String key){
                return false;
            }

            @Override
            public boolean hasBooleanDefault(String key){
                return false;
            }

        @Override
        public double defaultDoubleFor(String key){
            throw new IllegalStateException("No Double default found for key '"+key+"'");
        }

        @Override
        public boolean hasDoubleDefault(String key){
            return false;
        }
    };

}
