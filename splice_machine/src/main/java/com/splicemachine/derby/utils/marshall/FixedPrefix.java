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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * Prefix which always attaches the same byte[] to the front
 * of the hash.
 *
 * @author Scott Fines
 *         Date: 11/15/13
 */
public class FixedPrefix implements HashPrefix{
    private final byte[] prefix;

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public FixedPrefix(byte[] prefix){
        this.prefix=prefix;
    }

    @Override
    public int getPrefixLength(){
        return prefix.length;
    }

    @Override
    public void encode(byte[] bytes,int offset,byte[] hashBytes){
        System.arraycopy(prefix,0,bytes,offset,prefix.length);
    }

    @Override
    public void close() throws IOException{
    }
}
