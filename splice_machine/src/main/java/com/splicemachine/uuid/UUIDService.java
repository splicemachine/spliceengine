package com.splicemachine.uuid;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
public class UUIDService{
    private static volatile Snowflake snowflake;

    private UUIDService(){} //can't make me!

    public static UUIDGenerator newUuidGenerator(int batchSize){
        if(snowflake==null)
            return new BasicUUIDGenerator();
        else  return snowflake.newGenerator(batchSize);
    }

    public static void setSnowflake(Snowflake newSnowflake){
        snowflake = newSnowflake;
    }
}
