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

import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;

/**
 * @author Scott Fines
 *         Created on: 10/1/13
 */
public class SerializationUtils {

    private SerializationUtils(){}

    public static void writeNullableString(String value, DataOutput out) throws IOException {
        if (value != null) {
            out.writeBoolean(true);
            out.writeUTF(value);
        } else {
            out.writeBoolean(false);
        }
    }

    public static String readNullableString(ObjectInput in) throws IOException{
        if(in.readBoolean())
            return in.readUTF();
        return null;
    }
}
