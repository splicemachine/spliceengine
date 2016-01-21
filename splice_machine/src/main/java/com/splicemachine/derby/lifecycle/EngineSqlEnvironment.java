package com.splicemachine.derby.lifecycle;

import com.splicemachine.SqlEnvironment;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.uuid.Snowflake;

import java.sql.Connection;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public abstract class EngineSqlEnvironment implements SqlEnvironment{

    private SConfiguration config;
    private Snowflake snowflake;
    private Connection connection;
    private DatabaseVersion version;

    public EngineSqlEnvironment(){
    }

    @Override
    public void initialize(SConfiguration config,Snowflake snowflake,Connection internalConnection,DatabaseVersion spliceVersion){
        this.snowflake = snowflake;
        this.connection = internalConnection;
        this.version = spliceVersion;
        this.config = config;
    }

    @Override
    public Snowflake getUUIDGenerator(){
        return snowflake;
    }

    @Override
    public Connection getInternalConnection(){
        return connection;
    }

    @Override
    public DatabaseVersion getVersion(){
        return version;
    }

    @Override
    public SConfiguration getConfiguration(){
        return config;
    }
}
