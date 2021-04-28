/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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

package com.splicemachine.db.iapi.sql.compile.costing;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.impl.sql.compile.Predicate;

public interface ScanCostEstimator {

    /**
     * Add Predicate and keep track of the selectivity.
     */
    void addPredicate(Predicate p, double defaultSelectivityFactor) throws StandardException;

    /**
     * Compute the Base Scan Cost by utilizing the passed in StoreCostController
     */
    void generateCost(long numFirstIndexColumnProbes) throws StandardException;

    /**
     * Compute the Base Scan Cost of a single row.
     */
    void generateOneRowCost() throws StandardException;
}
