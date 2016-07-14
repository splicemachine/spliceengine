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

package com.splicemachine.derby.impl.sql;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.PropertyManager;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.impl.driver.SIDriver;

/**
 * @author Scott Fines
 *         Date: 1/27/16
 */
public class ZkPropertyManager implements PropertyManager{
    private String zkSpliceDerbyPropertyPath;

    public ZkPropertyManager(){
        zkSpliceDerbyPropertyPath =SIDriver.driver().getConfiguration().getSpliceRootPath()+HConfiguration.DERBY_PROPERTY_PATH;
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
