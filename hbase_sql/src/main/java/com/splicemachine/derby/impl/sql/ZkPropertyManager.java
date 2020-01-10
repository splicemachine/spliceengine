/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
