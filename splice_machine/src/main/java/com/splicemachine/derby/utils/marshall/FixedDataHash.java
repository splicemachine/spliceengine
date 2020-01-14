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

import com.splicemachine.db.iapi.error.StandardException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 11/18/13
 */
public class FixedDataHash<T> implements DataHash<T>{
    private final byte[] bytes;

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public FixedDataHash(byte[] bytes){
        this.bytes=bytes;
    }

    @Override
    public void setRow(T rowToEncode){
    }

    @Override
    public KeyHashDecoder getDecoder(){
        return null;
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public byte[] encode() throws StandardException, IOException{
        return bytes;
    }

    @Override
    public void close() throws IOException{

    }
}
