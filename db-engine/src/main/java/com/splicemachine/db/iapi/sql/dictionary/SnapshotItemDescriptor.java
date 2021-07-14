/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
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
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.sql.dictionary;

import org.joda.time.DateTime;

public class SnapshotItemDescriptor extends TupleDescriptor {
    private long snapshotId;
    private String snapshotName;
    private String schemaName;
    private String objectName;
    private long conglomerateNumber;
    private DateTime creationTime;

    public SnapshotItemDescriptor(long snapshotId,
                                  String snapshotName,
                                  String schemaName,
                                  String objectName,
                                  long conglomerateNumber,
                                  DateTime creationTime)
    {
        this.schemaName = schemaName;
        this.snapshotName = snapshotName;
        this.snapshotId = snapshotId;
        this.objectName = objectName;
        this.conglomerateNumber = conglomerateNumber;
        this.creationTime = creationTime;
    }

    public void setSnapshotId(long snapshotId)
    {
        this.snapshotId = snapshotId;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public long getSnapshotId()
    {
        return snapshotId;
    }

    public void setSchemaName(String schemaName)
    {
        this.schemaName = schemaName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public void setObjectName (String objectName)
    {
        this.objectName = objectName;
    }

    public String getObjectName()
    {
        return objectName;
    }

    public void setConglomerateNumber(long conglomerateNumber)
    {
        this.conglomerateNumber = conglomerateNumber;
    }

    public long getConglomerateNumber()
    {
        return conglomerateNumber;
    }

    public void setCreationTime(DateTime creationTime)
    {
        this.creationTime = creationTime;
    }

    public DateTime getCreationTime()
    {
        return creationTime;
    }
}
