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

package com.splicemachine.derby.utils;

import com.splicemachine.EngineDriver;
import com.splicemachine.hbase.jmx.JMXUtils;

import javax.management.*;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Implementation of wrapper class for JMX management statistics of Database Properties.
 *
 * @author David Winters
 *         Date: 4/23/2015
 */
public class DatabasePropertyManagementImpl implements DatabasePropertyManagement{

    // Registered instance of the JMX MBean.
    private static DatabasePropertyManagementImpl mBean=new DatabasePropertyManagementImpl();

    public static DatabasePropertyManagement instance(){
        return mBean;
    }

    /**
     * Register this implementation under JMX.
     *
     * @param mbs the MBeanServer to use
     * @throws MalformedObjectNameException
     * @throws NotCompliantMBeanException
     * @throws InstanceAlreadyExistsException
     * @throws MBeanRegistrationException
     */
    public static void registerJMX(MBeanServer mbs)
            throws MalformedObjectNameException,
            NotCompliantMBeanException,
            InstanceAlreadyExistsException,
            MBeanRegistrationException{
        mbs.registerMBean(mBean,new ObjectName(JMXUtils.DATABASE_PROPERTY_MANAGEMENT));
    }

    @Override
    public String getDatabaseProperty(String key) throws SQLException{
        Connection dbConn=getConnection();
        try(CallableStatement stmt=dbConn.prepareCall("VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY(?)")){
            stmt.setString(1,key);
            try(ResultSet rs=stmt.executeQuery()){
                if(rs.next()){
                    return rs.getString(1);
                }else return null;
            }
        }
    }

    @Override
    public void setDatabaseProperty(String key,String value) throws SQLException{
        Connection dbConn=getConnection();
        CallableStatement stmt=dbConn.prepareCall("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");
        try{
            stmt.setString(1,key);
            stmt.setString(2,value);
            stmt.executeUpdate();
        }finally{
            stmt.close();
        }
        dbConn.commit();
    }

    /**
     * Return a connection to the Splice database.
     *
     * @return a connection to the Splice database
     * @throws SQLException
     */
    private Connection getConnection() throws SQLException{
        //TODO -sf- this is almost certainly not secure
        return EngineDriver.driver().getInternalConnection();
//        EmbedConnectionMaker connMaker=new EmbedConnectionMaker();
//        return connMaker.createNew(new Properties());
    }
}
