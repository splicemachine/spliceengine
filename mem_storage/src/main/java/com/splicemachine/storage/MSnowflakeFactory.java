package com.splicemachine.storage;

import com.splicemachine.access.api.SnowflakeFactory;
import com.splicemachine.uuid.Snowflake;

/**
 *
 * Simple In Memory Snowflake Factory
 *
 * Created by jleach on 4/8/16.
 */
public class MSnowflakeFactory implements SnowflakeFactory {
    public static final MSnowflakeFactory INSTANCE= new MSnowflakeFactory();
    private MSnowflakeFactory(){}
    public Snowflake snowflake = new Snowflake((short)1);
    @Override
    public Snowflake getSnowFlake() {
        return snowflake;
    }
}
