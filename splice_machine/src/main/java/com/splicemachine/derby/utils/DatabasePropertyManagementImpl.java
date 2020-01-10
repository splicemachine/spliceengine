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
