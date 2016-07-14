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
public class BackupFileSetDescriptor extends TupleDescriptor {

    private String backupItem;
    private String regionName;
    private String fileName;
    private boolean include;

    public BackupFileSetDescriptor(String backupItem, String regionName, String fileName, boolean incldue) {
        this.backupItem = backupItem;
        this.regionName = regionName;
        this.fileName = fileName;
        this.include = include;
    }

    public String getBackupItem() {
        return backupItem;
    }

    public String getRegionName() {
        return regionName;
    }

    public String getFileName() {
        return fileName;
    }

    public boolean shouldInclude() {
        return include;
    }
}
