package com.splicemachine.derby.stream.spark;

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

import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.primitives.Bytes;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class SparkHBaseBulkImportRow implements Externalizable{
    private Long conglomerateId;
    private String key;
    private byte[] value;
    private int partition;

    public SparkHBaseBulkImportRow(){}

    public SparkHBaseBulkImportRow(Long conglomerateId,
                                   String key,
                                   byte[] value,
                                   int partition) {
        this.conglomerateId = conglomerateId;
        this.key = key;
        this.value = value;
        this.partition = partition;
    }

    public Long getConglomerateId() {
        return conglomerateId;
    }

    public String getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setConglomerateId(Long conglomerateId) {
        this.conglomerateId = conglomerateId;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public int getPartition() {
        return partition;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(conglomerateId);
        out.writeUTF(key);
        ArrayUtil.writeByteArray(out, value);
        out.writeInt(partition);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        conglomerateId = in.readLong();
        key = in.readUTF();
        value = ArrayUtil.readByteArray(in);
        partition = in.readInt();
    }

}
