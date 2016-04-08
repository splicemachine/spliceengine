package com.splicemachine.access.api;

import com.splicemachine.uuid.Snowflake;

/**
 * Created by jleach on 4/8/16.
 */
public interface SnowflakeFactory {

    public Snowflake getSnowFlake() throws Exception;

}
