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
 *
 */

package com.splicemachine.db.client.cluster;

import com.splicemachine.db.jdbc.ClientDataSource;
import com.splicemachine.db.jdbc.ClientDataSource40;

import javax.sql.DataSource;

/**
 * @author Scott Fines
 *         Date: 8/16/16
 */
public class ConfiguredServerPoolFactory implements ServerPoolFactory{
    private static final int DEFAULT_LOGIN_TIMEOUT = 1; //1 second to login before failing
    private static final int DEFAULT_VALIDATION_TIMEOUT = 1; //1 second to validate before failing

    private volatile int loginTimeout;
    private volatile int validationTimeout;
    private final FailureDetectorFactory failureDetectorFactory;
    private final PoolSizingStrategy sizingStrategy;
    private final String database;
    private final String user;
    private final String password;

    public ConfiguredServerPoolFactory(String database,String user, String password,
                                       FailureDetectorFactory failureDetectorFactory,
                                       PoolSizingStrategy sizingStrategy){
        this(database,user,password,DEFAULT_LOGIN_TIMEOUT,DEFAULT_VALIDATION_TIMEOUT,failureDetectorFactory,sizingStrategy);
    }

    @SuppressWarnings("WeakerAccess")
    public ConfiguredServerPoolFactory(String database,
                                       String user,String password,
                                       int loginTimeout,int validationTimeout,
                                       FailureDetectorFactory failureDetectorFactory,
                                       PoolSizingStrategy sizingStrategy){
        this.loginTimeout=loginTimeout;
        this.validationTimeout=validationTimeout;
        this.failureDetectorFactory=failureDetectorFactory;
        this.sizingStrategy = sizingStrategy;
        this.database = database;
        this.user = user;
        this.password = password;
    }

    @Override
    public ServerPool newServerPool(String serverId){
        int singleServerPoolSize = sizingStrategy.singleServerPoolSize();

        DataSource delegateDataSource=newDataSource(serverId,database,user,password);

        return new ServerPool(delegateDataSource,
                serverId,
                singleServerPoolSize,
                failureDetectorFactory.newFailureDetector(),
                sizingStrategy,
                validationTimeout);
    }

    /**
     * Create a new DataSource to a specific server (i.e. a single node data source).
     *
     * @param serverId the id of the server, of the form "[serverId]" or "[serverId]:[port]"
     * @return a DataSource to allow connections to that server
     */
    protected DataSource newDataSource(String serverId,String database,String user,String password){
        ClientDataSource delegateDataSource = new ClientDataSource40();
        delegateDataSource.setLoginTimeout(loginTimeout);
        String[] split=serverId.split(":");
        String serverName = split.length==2? split[0]: serverId;
        int port = split.length==2? Integer.parseInt(split[1]):1527;
        delegateDataSource.setServerName(serverName);
        delegateDataSource.setPortNumber(port);
        delegateDataSource.setDatabaseName(database);
        delegateDataSource.setUser(user);
        delegateDataSource.setPassword(password);
        return delegateDataSource;
    }

    void setLoginTimeout(int loginTimeout){
        this.loginTimeout = loginTimeout;
    }

    void setValidationTimeout(int validationTimeout){
        this.validationTimeout = validationTimeout;
    }

}
