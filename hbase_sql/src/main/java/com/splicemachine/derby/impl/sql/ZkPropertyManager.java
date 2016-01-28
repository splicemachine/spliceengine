package com.splicemachine.derby.impl.sql;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.PropertyManager;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 1/27/16
 */
public class ZkPropertyManager implements PropertyManager{
    private String zkSpliceDerbyPropertyPath;

    public ZkPropertyManager(){
        zkSpliceDerbyPropertyPath =SIDriver.driver().getConfiguration().getString(HConfiguration.SPLICE_ROOT_PATH)+HConfiguration.DERBY_PROPERTY_PATH;
    }

    @Override
    public boolean propertyExists(String propertyName) throws StandardException{
        return false;
    }

    @Override
    public Set<String> listProperties() throws StandardException{
        try{
            List<String> children=ZkUtils.getChildren(zkSpliceDerbyPropertyPath,false);
            return new HashSet<>(children);
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public String getProperty(String propertyName) throws StandardException{
        try{
            return Bytes.toString(ZkUtils.getData(zkSpliceDerbyPropertyPath+"/"+propertyName));
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void addProperty(String propertyName,String propertyValue) throws StandardException{
        try{
            ZkUtils.safeCreate(zkSpliceDerbyPropertyPath+"/"+propertyName,Bytes.toBytes(propertyValue),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void clearProperties() throws StandardException{
        try{
            List<String> children=ZkUtils.getChildren(zkSpliceDerbyPropertyPath,false);
            for(String child : children){
                ZkUtils.safeDelete(zkSpliceDerbyPropertyPath+"/"+child,-1);
            }
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }
}
