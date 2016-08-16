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

    public ConfiguredServerPoolFactory(FailureDetectorFactory failureDetectorFactory){
        this(DEFAULT_LOGIN_TIMEOUT,DEFAULT_VALIDATION_TIMEOUT,failureDetectorFactory);
    }

    @SuppressWarnings("WeakerAccess")
    public ConfiguredServerPoolFactory(int loginTimeout,int validationTimeout,FailureDetectorFactory failureDetectorFactory){
        this.loginTimeout=loginTimeout;
        this.validationTimeout=validationTimeout;
        this.failureDetectorFactory=failureDetectorFactory;
    }

    @Override
    public ServerPool newServerPool(String serverId,PoolSizingStrategy sizingStrategy,BlackList<ServerPool> blacklist){
        int singleServerPoolSize = sizingStrategy.singleServerPoolSize();

        DataSource delegateDataSource=newDataSource(serverId);

        return new ServerPool(delegateDataSource,
                serverId,
                singleServerPoolSize,
                failureDetectorFactory.newFailureDetector(),
                sizingStrategy,
                blacklist,
                validationTimeout);
    }

    /**
     * Create a new DataSource to a specific server (i.e. a single node data source).
     *
     * @param serverId the id of the server, of the form "[serverId]" or "[serverId]:[port]"
     * @return a DataSource to allow connections to that server
     */
    protected DataSource newDataSource(String serverId){
        ClientDataSource delegateDataSource = new ClientDataSource40();
        delegateDataSource.setLoginTimeout(loginTimeout);
        String[] split=serverId.split(":");
        String serverName = split.length==2? split[0]: serverId;
        int port = split.length==2? Integer.parseInt(split[1]):1527;
        delegateDataSource.setServerName(serverName);
        delegateDataSource.setPortNumber(port);
        return delegateDataSource;
    }

    void setLoginTimeout(int loginTimeout){
        this.loginTimeout = loginTimeout;
    }

    void setValidationTimeout(int validationTimeout){
        this.validationTimeout = validationTimeout;
    }

}
