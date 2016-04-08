package com.splicemachine.access.hbase;

import com.splicemachine.access.api.SnowflakeFactory;
import com.splicemachine.hbase.ZkUtils;
import com.splicemachine.uuid.Snowflake;

/**
 * Created by jleach on 4/8/16.
 */
public class HSnowflakeFactory implements SnowflakeFactory{
    @Override
    public Snowflake getSnowFlake() throws Exception {
        return new Snowflake(ZkUtils.assignSnowFlakeSequence());
    }
}
