/*
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.iapi.sql.dictionary;

/**
 * Created by jyuan on 2/6/15.
 */
import org.joda.time.DateTime;

import java.io.*;

public class BackupDescriptor extends TupleDescriptor{

    private long backupId;
    private DateTime beginTimestamp;
    private DateTime endTimestamp;
    private String status;
    private String fileSystem;
    private String scope;
    private boolean isIncremental;
    private long parentId;
    private int items;

    public BackupDescriptor() {}

    public BackupDescriptor(long backupId,
                            DateTime beginTimestamp,
                            DateTime endTimestamp,
                            String status,
                            String fileSystem,
                            String scope,
                            boolean isIncremental,
                            long parentId,
                            int items) {
        this.backupId = backupId;
        this.beginTimestamp = beginTimestamp;
        this.endTimestamp = endTimestamp;
        this.status = status;
        this.fileSystem = fileSystem;
        this.scope = scope;
        this.isIncremental = isIncremental;
        this.parentId = parentId;
        this.items = items;
    }

    public long getBackupId() {
        return backupId;
    }

    public DateTime getBeginTimestamp() {
        return beginTimestamp;
    }

    public DateTime getEndTimestamp() {
        return endTimestamp;
    }

    public String getStatus() {
        return status;
    }

    public String getFileSystem() {
        return fileSystem;
    }

    public String getScope() {
        return scope;
    }

    public boolean isIncremental() {
        return isIncremental;
    }

    public long getParentBackupId() {
        return parentId;
    }

    public int getItems() {
        return items;
    }

    public void writeExternal(DataOutput out) throws IOException {
        out.writeLong(backupId);
        out.writeLong(beginTimestamp.getMillis());
        out.writeLong(endTimestamp.getMillis());
        out.writeUTF(status);
        out.writeUTF(fileSystem);
        out.writeUTF(scope);
        out.writeBoolean(isIncremental);
        out.writeLong(parentId);
        out.writeInt(items);
    }

    public void readExternal(DataInput in) throws IOException {
        backupId = in.readLong();
        beginTimestamp = new DateTime(in.readLong());
        endTimestamp = new DateTime(in.readLong());
        status = in.readUTF();
        fileSystem = in.readUTF();
        scope = in.readUTF();
        isIncremental = in.readBoolean();
        parentId = in.readLong();
        items = in.readInt();
    }
}
