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

package com.splicemachine.pipeline.contextfactory;

/*
 * State management indicator
 */
enum State {
    /*
     * Mutations which see this state will emit a warning, but will still be allowed through.
     * Allowing mutations through during this phase prevents deadlocks when initialized the Database
     * for the first time. After the Derby DataDictionary is properly instantiated, no index set should
     * remain in this state for long.
     */
    WAITING_TO_START,
    /*
     * Indicates that the Index Set is able to start whenever it feels like it (typically before the
     * first mutation to the main table). IndexSets in this state believe that the Derby DataDictionary
     * has properly instantiated.
     */
    READY_TO_START,
    /*
     * Indicates that this set is currently in the process of intantiating all the indices, constraints, and
     * so forth, and that all mutations should block until set up is complete.
     */
    STARTING,
    /*
     * Indicates that initialization failed in some fatal way. Mutations encountering this state
     * will blow back on the caller with a DoNotRetryIOException
     */
    FAILED_SETUP,
    /*
     * The IndexSet is running, and index management is going smoothly
     */
    RUNNING,
    /*
     * the IndexSet is shutdown. Mutations will explode upon encountering this.
     */
    SHUTDOWN,
    /*
     * This IndexSet does not manage Mutations.
     */
    NOT_MANAGED
}
