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

package com.splicemachine.tools;

import com.splicemachine.db.agg.Aggregator;
import com.splicemachine.db.iapi.error.StandardException;

public class StringConcat implements Aggregator<String, String, StringConcat> {
    private StringBuilder agg;

    public StringConcat() {
    }

    @Override
    public void init() {
        agg = new StringBuilder();
    }

    @Override
    public void accumulate(String value) throws StandardException {
        agg.append(value);
    }

    @Override
    public void merge(StringConcat otherAggregator) {
        agg.append(otherAggregator.agg);
    }

    @Override
    public String terminate() {
        return agg.toString();
    }
}