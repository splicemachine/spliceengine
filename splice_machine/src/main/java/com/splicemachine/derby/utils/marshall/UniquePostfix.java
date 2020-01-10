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
