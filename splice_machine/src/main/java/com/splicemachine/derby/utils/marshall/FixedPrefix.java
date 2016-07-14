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
