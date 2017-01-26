/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
import com.splicemachine.derby.utils.StandardSupplier;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @author Scott Fines
 *         Date: 11/18/13
 */
public class GeneratedBytesSupplier implements StandardSupplier<byte[]>{
    private final byte[] baseBytes;
    private final int offset;
    private final StandardSupplier<byte[]> changingBytesSupplier;

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public GeneratedBytesSupplier(byte[] baseBytes,int offset,StandardSupplier<byte[]> changingBytesSupplier){
        this.baseBytes=baseBytes;
        this.offset=offset;
        this.changingBytesSupplier=changingBytesSupplier;
    }

    @Override
    public byte[] get() throws StandardException{
        byte[] changedBytes=changingBytesSupplier.get();
        System.arraycopy(changedBytes,0,baseBytes,offset,changedBytes.length);
        return changedBytes;
    }
}
