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

package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.utils.ByteDataInput;
import com.splicemachine.utils.ByteDataOutput;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import java.io.IOException;


public class DerbyBytesUtil {
    private static Logger LOG = Logger.getLogger(DerbyBytesUtil.class);

    @SuppressWarnings("unchecked")
    public static <T> T fromBytes(byte[] bytes) throws StandardException {
        try {
            return fromBytesUnsafe(bytes);
        } catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG, "fromBytes Exception", Exceptions.parseException(e));
            return null; //can't happen
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T fromBytesUnsafe(byte[] bytes) throws IOException, ClassNotFoundException {
        try(ByteDataInput bdi = new ByteDataInput(bytes)){
            return (T) bdi.readObject();
        }
    }

    public static byte[] toBytes(Object object) throws StandardException {
        try(ByteDataOutput bdo = new ByteDataOutput()){
            bdo.writeObject(object);
            return bdo.toByteArray();
        } catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG, "fromBytes Exception", Exceptions.parseException(e));
            return null;
        }
    }



}
