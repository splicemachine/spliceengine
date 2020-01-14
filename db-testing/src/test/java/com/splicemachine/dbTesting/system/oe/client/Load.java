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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.dbTesting.system.oe.client;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface for a client to populate the database. Various implementations can
 * be provided, e.g. via SQL, via VTI, via import etc
 * An implementation for Load must be able to do the following
 * <OL>
 * <LI> Use the setupLoad to perform any necessary initialization for the load
 * phase
 * <LI> Load data into all the tables 
 * </OL>
 * <P>
 * DECIMAL values are represented as String objects to allow Order Entry to be
 * run on J2ME/CDC/Foundation which does not support BigDecimal.
 */
public interface Load {

    /**
     * Cardinality constants 
     * These numbers are factored by W, the number of Warehouses,
     * to illustrate the database scaling.
     * 
     * see section 1.2.1 of TPC-C specification
     */
    public static final short DISTRICT_COUNT_W = 10;

    public static final int CUSTOMER_COUNT_W = 3000 * DISTRICT_COUNT_W;

    public static final int ITEM_COUNT = 100000;

    public static final int NEWORDERS_COUNT_W = (CUSTOMER_COUNT_W * 9) / 30;

    // 1 per customer initially
    public static final int HISTORY_COUNT_W = CUSTOMER_COUNT_W; 

    public static final int STOCK_COUNT_W = ITEM_COUNT;

    public static final int ORDERS_COUNT_W = CUSTOMER_COUNT_W;

    //  5 to 15 , per order. so average 10
    public static final int ORDERLINE_COUNT_WV = ORDERS_COUNT_W * 10; 

    public static final int NEWORDERS_BREAKPOINT = 
        (ORDERS_COUNT_W - NEWORDERS_COUNT_W)/ DISTRICT_COUNT_W;
    
    
    /**
     * Set the seed for the random number generator used to
     * populate the data. Useful for testing to ensure consistent
     * repeatable runs. If not set, defaults a value based upon current time.
     * Must be called before setupLoad to have an effect.
     * @param seed
     */
    public void setSeed(long seed);

    /**
     * Perform the necessary setup before database population.
     * @param conn - database connection 
     * @param scale - scale of the database.  The WAREHOUSE table is 
     * used as the base unit of scaling. 
     * @throws Exception
     */
    public void setupLoad(Connection conn, short scale) throws Exception;
    
    /**
     * Set the number of total threads the loader is allowed to use
     * to load data.
     * If an implementation does not support multiple threads then
     * the passed in value will be ignored.
     * 
     * @param threadCount Number of threads to use if loading supports
     * multiple threading.
     */
    public void setThreadCount(int threadCount);

    /**
     * Follow the initial database population requirements in Section 4.3.3 
     * and populate all the required tables.
     * BE CAREFUL to use the correct starting identifiers for the data in
     * the tables. In the specification, identifiers start at 1 (one), 
     * e.g. 1-10 for a district and is not zero based.
     * 
     * @throws SQLException
     */
    public void populateAllTables() throws Exception;

}
