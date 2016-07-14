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
