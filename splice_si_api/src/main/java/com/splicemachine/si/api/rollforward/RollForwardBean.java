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
 *
 */

package com.splicemachine.si.api.rollforward;

import com.splicemachine.annotations.Description;
import com.splicemachine.annotations.PName;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.ByteSlice;

import javax.management.MXBean;

@MXBean
public interface RollForwardBean {


    /**
     * Returns the number of elements on the first queue.
     *
     * @return number of elements on the first queue.
     */
    @Description(value="Get the number of elements on the first queue.")
    int getFirstQueueSize();

    /**
     * Returns the number of elements on the second queue.
     *
     * @return number of elements on the second queue.
     */
    @Description(value="Get the number of elements on the second queue.")
    int getSecondQueueSize();


    /**
     * Returns the number of successful resolutions on the first queue.
     *
     * @return number of successful resolutions on the first queue.
     */
    @Description(value="Get the number of successful resolutions on the first queue.")
    long getFirstQueueResolutions();

    /**
     * Returns the number of successful resolutions on the second queue.
     *
     * @return number of successful resolutions on the second queue.
     */
    @Description(value="Get the number of successful resolutions on the second queue.")
    long getSecondQueueResolutions();

    /**
     * Returns the number of still active transactions on the first queue.
     *
     * @return number of still active transactions on the first queue.
     */
    @Description(value="Get the number of still active transactions on the first queue.")
    long getFirstQueueActive();

    /**
     * Returns the number of still active transactions on the second queue.
     *
     * @return number of still active transactions on the second queue.
     */
    @Description(value="Get the number of still active transactions on the second queue.")
    long getSecondQueueActive();
}
