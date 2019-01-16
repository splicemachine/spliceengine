/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
package com.splicemachine.derby.iapi.sql.execute;

import java.util.Date;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;

/**
 *  Entity for storing running operations' info
 *  (Operation, Thread, Submitted Time, Running Engine)
 *
 *  @author Changli Liu
 *  @time 01/29/2018
 */

public class RunningOperation{

    private Date submittedTime = null;
    private DataSetProcessor.Type engine = null;
    private SpliceOperation operation;
    private Thread thread;

    public RunningOperation(SpliceOperation operation,
                            Thread thread,
                            Date submittedTime,
                            DataSetProcessor.Type engine) {
        this.operation = operation;
        this.thread =thread;
        this.submittedTime = submittedTime;
        this.engine = engine;
    }

    public Date getSubmittedTime() {
        return submittedTime;
    }

    public DataSetProcessor.Type getEngine() {
        return engine;
    }

    public SpliceOperation getOperation() {
        return operation;
    }

    public Thread getThread() {
        return thread;
    }

}