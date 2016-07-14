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
