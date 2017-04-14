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

package com.splicemachine.hbase;

import java.io.*;

/**
 * Created by jyuan on 5/5/17.
 */
public class BackupJobStatus implements Externalizable{
    private long backupId;
    private boolean isIncremental = false;
    private long lastActiveTimestamp = 0;

    public BackupJobStatus(){}

    public BackupJobStatus(long backupId, boolean isIncremental,long lastActiveTimestamp) {
        this.backupId = backupId;
        this.isIncremental = isIncremental;
        this.lastActiveTimestamp = lastActiveTimestamp;
    }

    public long getBackupId() {
        return backupId;
    }

    public long getLastActiveTimestamp() {
        return lastActiveTimestamp;
    }

    public void setLastActiveTimestamp(long lastActiveTimestamp) {
        this.lastActiveTimestamp = lastActiveTimestamp;
    }

    public boolean isIncremental() {
        return isIncremental;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(backupId);
        out.writeBoolean(isIncremental);
        out.writeLong(lastActiveTimestamp);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        backupId = in.readLong();
        isIncremental = in.readBoolean();
        lastActiveTimestamp = in.readLong();
    }

    /**
     *
     * @return byte representation of job status
     * @throws IOException
     */
    public byte[] toBytes() throws IOException{
        ObjectOutput out = null;
        ByteArrayOutputStream bos = null;
        try {
            bos = new ByteArrayOutputStream();
            out = new ObjectOutputStream(bos);
            out.writeObject(this);
            out.flush();
            byte[] b = bos.toByteArray();

            return b;
        } finally {
            if (bos != null) {
                bos.close();
            }
        }
    }

    /**
     *
     * @param bs BackupJobStatus in bytes
     * @return a BackupJobStatus object
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public static BackupJobStatus parseFrom(byte[] bs) throws  IOException, ClassNotFoundException {
        ByteArrayInputStream bis = new ByteArrayInputStream(bs);
        ObjectInput in = null;
        try {
            in = new ObjectInputStream(bis);
            BackupJobStatus backupJobStatus = (BackupJobStatus) in.readObject();
            return backupJobStatus;
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }
}
