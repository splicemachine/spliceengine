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

package com.splicemachine.timestamp.api;

import javax.management.MXBean;

/**
 * Interface for exposing Timestamp Generator related metrics
 * via JMX, for the master server.
 * 
 * @author Walt Koetke
 */

@MXBean
public interface TimestampOracleStatistics{

    /**
     * Returns the total number of timestamps that have been created
     * since the last master server start.
     * 
     * @return number of timestamps created
     */
    long getNumberTimestampsCreated();
	
    /**
     * Returns the total number of timestamp 'blocks' reserved
     * since the last master server start.
     * 
     * @return number of timestamp blocks reserved
     */
    long getNumberBlocksReserved();
	
}
