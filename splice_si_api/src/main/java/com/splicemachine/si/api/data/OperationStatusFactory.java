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
 */

package com.splicemachine.si.api.data;

import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.storage.MutationStatus;

import java.io.IOException;

/**
 * Factory for creating the proper MutationStatus for the given architecture.
 *
 * Created by jleach on 12/9/15.
 */
public interface OperationStatusFactory{

    /**
     * Return true if all is good, throw status specific exception if not.
     *
     * @return {@code true} if there is no error thrown
     * @throws IOException
     */
    boolean processPutStatus(MutationStatus operationStatus) throws IOException;

    MutationStatus getCorrectStatus(MutationStatus status,MutationStatus oldStatus);

    MutationStatus success();

    MutationStatus notRun();

    MutationStatus failure(String message);

    MutationStatus failure(Throwable t);

    ConstraintChecker getNoOpConstraintChecker();
}
