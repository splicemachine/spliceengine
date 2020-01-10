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

package com.splicemachine.si.api.filter;

import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.DataFilter;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 4/9/14
 */
public interface SIFilter{

    /**
     * Reset the filter for the next row.
     */
    void nextRow();

    /**
     * @return the accumulator used in the filter
     */
    RowAccumulator getAccumulator();

    DataFilter.ReturnCode filterCell(DataCell kv) throws IOException;
}
