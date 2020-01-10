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
 *
 */

package com.splicemachine.si.api.txn;

public class TaskId {
    private final int stageId;
    private final int partitionId;
    private final int taskAttemptNumber;

    public TaskId(int stageId, int partitionId, int taskAttemptNumber) {
        this.stageId = stageId;
        this.partitionId = partitionId;
        this.taskAttemptNumber = taskAttemptNumber;
    }

    public int getStageId() {
        return stageId;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public int getTaskAttemptNumber() {
        return taskAttemptNumber;
    }

    public boolean sameTask(TaskId taskId) {
        return stageId == taskId.stageId &&
                partitionId == taskId.partitionId;
    }

    public boolean isRetry(TaskId otherTaskId) {
        return sameTask(otherTaskId) && taskAttemptNumber > otherTaskId.taskAttemptNumber;
    }
}
