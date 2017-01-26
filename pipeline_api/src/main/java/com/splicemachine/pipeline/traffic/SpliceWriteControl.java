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

package com.splicemachine.pipeline.traffic;

/**
 * @author Scott Fines
 *         Date: 1/15/16
 */
public interface SpliceWriteControl{
    enum Status {
        DEPENDENT, INDEPENDENT,REJECTED
    }

    Status performDependentWrite(int writes);

    boolean finishDependentWrite(int writes);

    Status performIndependentWrite(int writes);

    boolean finishIndependentWrite(int writes);

    WriteStatus getWriteStatus();

    int maxDependendentWriteThreads();

    int maxIndependentWriteThreads();

    int maxDependentWriteCount();

    int maxIndependentWriteCount();

    void setMaxIndependentWriteThreads(int newMaxIndependentWriteThreads);

    void setMaxDependentWriteThreads(int newMaxDependentWriteThreads);

    void setMaxIndependentWriteCount(int newMaxIndependentWriteCount);

    void setMaxDependentWriteCount(int newMaxDependentWriteCount);
}
