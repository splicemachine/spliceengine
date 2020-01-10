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

package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.services.io.ArrayUtil;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jyuan on 3/20/17.
 */
public class BulkImportPartition implements Externalizable {
    private Long conglomerateId;
    private String regionName;
    private byte[] startKey;
    private byte[] endKey;
    private String filePath;

    public BulkImportPartition() {}

    public BulkImportPartition(Long conglomerateId,
                               String regionName,
                               byte[] startKey,
                               byte[] endKey,
                               String filePath) {
        this.conglomerateId = conglomerateId;
        this.regionName = regionName;
        this.startKey = startKey;
        this.endKey = endKey;
        this.filePath = filePath;
    }

    public Long getConglomerateId() {
        return conglomerateId;
    }

    public String getFilePath() {
        return filePath;
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }

    public String getRegionName() {
        return regionName;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(conglomerateId);
        out.writeUTF(regionName);
        ArrayUtil.writeByteArray(out, startKey);
        ArrayUtil.writeByteArray(out, endKey);
        out.writeUTF(filePath);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        conglomerateId = in.readLong();
        regionName = in.readUTF();
        startKey = ArrayUtil.readByteArray(in);
        endKey = ArrayUtil.readByteArray(in);
        filePath = in.readUTF();
    }
}
