/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
package com.splicemachine.storage;

/**
 *
 * Record Type
 *
 */
public enum RecordType {
    INSERT((byte)0x01),
    UPDATE((byte)0x02),
    DELETE((byte)0x03),
    UPSERT((byte)0x05),
    CANCEL((byte)0x08); /* For import process to cancel out an inserted row that violates a unique constraint */

    private final byte typeCode;

    private RecordType(byte typeCode) { this.typeCode = typeCode; }

    public static RecordType decode(byte typeByte) {
        for(RecordType type:values()){
            if(type.typeCode==typeByte) return type;
        }
        throw new IllegalArgumentException("Incorrect typeByte "+ typeByte);
    }

    public byte asByte() {
        return typeCode;
    }

    public boolean isUpdateOrUpsert() {
        return UPDATE.equals(this) || UPSERT.equals(this);
    }
}
