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

import org.joda.time.DateTime;

/**
 * Created by jyuan on 3/24/15.
 */
public class BackupJobsDescriptor extends TupleDescriptor{

    private long jobId;
    private String fileSystem;
    private String type;
    private int hourOfDay;
    private DateTime beginTimestamp;

    public BackupJobsDescriptor () {}

    public BackupJobsDescriptor (long jobId,
                                 String fileSystem,
                                 String type,
                                 int hourOfDay,
                                 DateTime beginTimestamp) {
        this.jobId = jobId;
        this.fileSystem = fileSystem;
        this.type = type;
        this.hourOfDay = hourOfDay;
        this.beginTimestamp = beginTimestamp;
    }

    public long getJobId() {
        return jobId;
    }

    public String getFileSystem() {
        return fileSystem;
    }

    public String getType() {
        return type;
    }

    public int getHourOfDay() {
        return hourOfDay;
    }

    public DateTime getBeginTimestamp() {
        return beginTimestamp;
    }
}
