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

package com.splicemachine.concurrent;

import java.util.concurrent.locks.Lock;

/**
 * A Factory for constructing locks. This is useful when you want to replace different lock implementations
 * (i.e. for simulating lock timeout etc. without relying on the system clock).
 *
 * @author Scott Fines
 *         Date: 9/4/15
 */
public interface LockFactory{

    /**
     * @return a new lock from the factory
     */
    Lock newLock();
}
