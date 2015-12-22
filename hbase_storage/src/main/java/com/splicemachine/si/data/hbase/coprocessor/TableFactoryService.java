package com.splicemachine.si.data.hbase.coprocessor;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.si.impl.TxnNetworkLayerFactory;
import org.apache.hadoop.hbase.TableName;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public class TableFactoryService{
    @SuppressWarnings("unchecked")
    public static PartitionFactory<TableName> loadTableFactory(){
        ServiceLoader<PartitionFactory> serviceLoader = ServiceLoader.load(PartitionFactory.class);
        Iterator<PartitionFactory> iter = serviceLoader.iterator();
        if(!iter.hasNext())
            throw new IllegalStateException("No TableFactory found!");

        PartitionFactory stf = iter.next();
        return (PartitionFactory<TableName>)stf;
    }

    public static TxnNetworkLayerFactory loadTxnNetworkLayer(){
        ServiceLoader<TxnNetworkLayerFactory> serviceLoader = ServiceLoader.load(TxnNetworkLayerFactory.class);
        Iterator<TxnNetworkLayerFactory> iter = serviceLoader.iterator();
        if(!iter.hasNext())
            throw new IllegalStateException("No TableFactory found!");

        return iter.next();
    }
}
