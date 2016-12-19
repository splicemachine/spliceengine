/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.si.api.data;

import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.storage.MutationStatus;

import java.io.IOException;

/**
 * Factory for creating the proper MutationStatus for the given architecture.
 *
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
