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

package com.splicemachine.derby.utils.marshall;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.uuid.Snowflake;
import com.splicemachine.uuid.UUIDGenerator;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * Postfix which uses UUIDs to ensure that the postfix is unique.
 *
 * @author Scott Fines
 *         Date: 11/18/13
 */
public class UniquePostfix implements KeyPostfix{
    private final byte[] baseBytes;
    private final UUIDGenerator generator;

    public UniquePostfix(byte[] baseBytes){
        this(baseBytes,EngineDriver.driver().newUUIDGenerator(100));
    }

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public UniquePostfix(byte[] baseBytes,UUIDGenerator generator){
        this.generator=generator;
        this.baseBytes=baseBytes;
    }

    @Override
    public int getPostfixLength(byte[] hashBytes) throws StandardException{
        return baseBytes.length+Snowflake.UUID_BYTE_SIZE;
    }

    @Override
    public void encodeInto(byte[] keyBytes,int postfixPosition,byte[] hashBytes){
        byte[] uuidBytes=generator.nextBytes();
        System.arraycopy(uuidBytes,0,keyBytes,postfixPosition,uuidBytes.length);
        System.arraycopy(baseBytes,0,keyBytes,postfixPosition+uuidBytes.length,baseBytes.length);
    }

    @Override
    public void close() throws IOException{
    }
}
