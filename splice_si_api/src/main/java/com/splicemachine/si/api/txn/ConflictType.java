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

package com.splicemachine.si.api.txn;

/**
 * Indicates whether/how two transaction's writes would or do conflict with each other.
 */
public enum ConflictType {
    NONE, // the two transactions do not and would not conflict
    SIBLING, // the two transactions are effectively siblings of each other, meaning one does not contain the other, but they would or do conflict
    CHILD, // one transaction is a descendant of the other and they would or do conflict
    /**
     * Additive Conflicts occur when two additive transactions interact with one another. Generally,
     * Additive conflicts are ignored (because they don't truly "conflict" in the same way as other conflicts),
     * but occasionally (as in the case of UPSERTs) we want to recognize that we had an additive conflict and
     * deal with it.
     */
    ADDITIVE,
}
