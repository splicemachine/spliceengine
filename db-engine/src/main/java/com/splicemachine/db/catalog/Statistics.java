/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.catalog;

/**
 
 <P>
 This interface is used in the column SYS.SYSSTATISTICS.STATISTICS. It
 encapsulates information collected by the UPDATE STATISTICS command
 and is used internally by the Derby optimizer to estimate cost 
 and selectivity of different query plans.
 <p>
*/

public interface Statistics {
    /**
     * @return the UUID of the <em>backing conglomerate</em> which this
     * statistics entity describes (e.g. the table heap, index, and so on)
     */
    long getConglomerateId();

    /**
     * Returns the estimated number of rows in the index.
     *
     * @return Number of rows.
     */
    long getRowEstimate();

	/**
	 * @return the selectivity for a set of predicates.
	 */
	double selectivity(Object[] predicates);

    int getColumnCount();
}
