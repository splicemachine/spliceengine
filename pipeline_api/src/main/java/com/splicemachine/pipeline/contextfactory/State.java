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
