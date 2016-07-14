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

package com.splicemachine.storage;

/**
 * Type of Predicate used. Used for efficient serialization.
 *
 * @author Scott Fines
 * Created on: 8/12/13
 */
public enum PredicateType {
    VALUE((byte)0x01),
    NULL((byte)0x02),
    AND((byte)0x03),
    OR((byte)0x04),
    CUSTOM((byte)0x05),
		CHAR_VALUE((byte)0x06);

    private final byte type;

    private PredicateType(byte type) {
        this.type = type;
    }

    public byte byteValue(){
        return type;
    }

    public static PredicateType valueOf(byte typeByte) {
				if(CHAR_VALUE.type==typeByte)
						return CHAR_VALUE;
        if(VALUE.type==typeByte)
            return VALUE;
        else if(NULL.type==typeByte)
            return NULL;
        else if(AND.type==typeByte)
            return AND;
        else if(OR.type==typeByte)
            return OR;
        else
            return CUSTOM;
    }
}
