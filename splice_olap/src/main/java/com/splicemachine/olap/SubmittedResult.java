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

package com.splicemachine.olap;

import com.splicemachine.derby.iapi.sql.olap.OlapResult;

/**
 * @author Scott Fines
 *         Date: 4/4/16
 */
public class SubmittedResult implements OlapResult{
    private static final long serialVersionUID = 1l;
    private long tickTime;

    public SubmittedResult(){
    }

    public SubmittedResult(long tickTime){
        this.tickTime=tickTime;
    }

    public long getTickTime(){
        return tickTime;
    }

    @Override public boolean isSuccess(){ return false; }

    @Override
    public Throwable getThrowable(){
        return null;
    }
}
