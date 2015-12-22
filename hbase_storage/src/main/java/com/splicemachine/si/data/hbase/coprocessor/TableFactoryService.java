package com.splicemachine.si.data.hbase.coprocessor;

import com.splicemachine.access.api.STableFactory;
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
    public static STableFactory<TableName> loadTableFactory(){
        ServiceLoader<STableFactory> serviceLoader = ServiceLoader.load(STableFactory.class);
        Iterator<STableFactory> iter = serviceLoader.iterator();
        if(!iter.hasNext())
            throw new IllegalStateException("No TableFactory found!");

        STableFactory stf = iter.next();
        return (STableFactory<TableName>)stf;
    }

    public static TxnNetworkLayerFactory loadTxnNetworkLayer(){
        ServiceLoader<TxnNetworkLayerFactory> serviceLoader = ServiceLoader.load(TxnNetworkLayerFactory.class);
        Iterator<TxnNetworkLayerFactory> iter = serviceLoader.iterator();
        if(!iter.hasNext())
            throw new IllegalStateException("No TableFactory found!");

        return iter.next();
    }
}
