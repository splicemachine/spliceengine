/*
 * Copyright 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.backup;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.db.iapi.services.io.ArrayUtil;

import java.io.*;
import java.util.List;

/**
 * Created by jyuan on 6/27/19.
 */
public class BackupRegionStatus implements Externalizable {
    private byte[] startKey;
    private byte[] endKey;
    private byte[] status;
    private List<String> backupFiles= Lists.newArrayList();

    public BackupRegionStatus() {}

    public BackupRegionStatus(byte[] startKey, byte[] endKey, byte[] status) {
        this.startKey = startKey;
        this.endKey = endKey;
        this.status = status;
    }

    public List<String> getBackupFiles() {
        return backupFiles;
    }

    public void clearBackupFiles() {
        backupFiles.clear();
    }

    public void addBackupFiles(List<String> backupFiles) {
        this.backupFiles.addAll(backupFiles);
    }

    public byte[] getStatus() {
        return status;
    }

    public void setStatus(byte[] status) {
        this.status = status;
    }

    public byte[] toBytes() throws IOException {
        try(ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(this);
            out.flush();
            byte[] b = bos.toByteArray();

            return b;
        }
    }

    public static BackupRegionStatus parseFrom (byte[] bs) throws IOException, ClassNotFoundException{
        try(ByteArrayInputStream bis = new ByteArrayInputStream(bs);
            ObjectInput in = new ObjectInputStream(bis)) {
            BackupRegionStatus backupRegionStatus = (BackupRegionStatus) in.readObject();
            return backupRegionStatus;
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        ArrayUtil.writeByteArray(out, startKey);
        ArrayUtil.writeByteArray(out, endKey);
        ArrayUtil.writeByteArray(out, status);
        out.writeInt(backupFiles.size());
        for(String backFile : backupFiles) {
            out.writeUTF(backFile);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        startKey = ArrayUtil.readByteArray(in);
        endKey = ArrayUtil.readByteArray(in);
        status = ArrayUtil.readByteArray(in);
        int n = in.readInt();
        backupFiles = Lists.newArrayList();
        for (int i = 0; i < n; ++i) {
            backupFiles.add(in.readUTF());
        }
    }

    public byte[] getStartKey() {
        return startKey;
    }

    public byte[] getEndKey() {
        return endKey;
    }
}
