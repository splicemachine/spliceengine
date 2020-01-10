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
