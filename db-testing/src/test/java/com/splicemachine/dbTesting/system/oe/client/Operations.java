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
package com.splicemachine.dbTesting.system.oe.client;

/**
 * Interface for a client to execute the logical operations. Various
 * implementations can be provided, e.g. client side SQL, procedure, etc.
 * <P>
 * Typical model is that each client has its own instance of an object that
 * implements Operations. For example the implementation in a client side SQL
 * implementation would have a reference to its own JDBC connection and prepared
 * statements.
 * <P>
 * Implementations of the execution methods must perform the following:
 * <OL>
 * <LI>Execute business transaction
 * <LI>Populate POJO objects required by display method
 * <LI>Commit the database transaction(s)
 * <LI>Call the appropriate display method from Display
 * </UL>
 * 
 * <P>
 * DECIMAL values are represented as String objects to allow Order Entry to be
 * run on J2ME/CDC/Foundation which does not support BigDecimal.
 */

public interface Operations {

    /**
     * Execute stock level. Stock level is described in clause 2.8.
     * 
     * @param display
     *            Where to display the results, if null results are not
     *            displayed.
     * @param displayData
     *            Any client specific display information, such as servlet
     *            context.
     * @param w
     *            Warehouse for transaction
     * @param d
     *            District for transaction
     * @param threshold
     *            Threshold for transaction.
     * @see Display#displayStockLevel(Object, short, short, int, int)
     */
    public void stockLevel(Display display, Object displayData, short w,
            short d, int threshold) throws Exception;

    /**
     * Execute order status by last name. Order status is described in clause
     * 2.6.
     * 
     * @param display
     *            Where to display the results, if null results are not
     *            displayed.
     * @param displayData
     *            Any client specific display information, such as servlet
     *            context.
     * @param w
     *            Warehouse identifier
     * @param d
     *            District identifier
     * @param customerLast
     *            Customer's last name.
     */
    public void orderStatus(Display display, Object displayData, short w,
            short d, String customerLast) throws Exception;

    /**
     * Execute order status by customer identifer. Order status is described in
     * clause 2.6.
     * 
     * @param display
     *            Where to display the results, if null results are not
     *            displayed.
     * @param displayData
     *            Any client specific display information, such as servlet
     *            context.
     * @param w
     *            Warehouse identifier
     * @param d
     *            District identifier
     * @param c
     *            Customer identifer.
     */
    public void orderStatus(Display display, Object displayData, short w,
            short d, int c) throws Exception;

    /**
     * Execute payment by last name. Payment is described in clause 2.5.
     * 
     * @param display
     *            Where to display the results, if null results are not
     *            displayed.
     * @param displayData
     *            Any client specific display information, such as servlet
     *            context.
     * @param w
     *            Home warehouse identifier
     * @param d
     *            Home district identifier
     * @param cw
     *            Customer warehouse identifier
     * @param cd
     *            Customer district identifier
     * @param customerLast
     *            Customer's last name.
     * @param amount
     *            Payment amount
     */
    public void payment(Display display, Object displayData, short w, short d,
            short cw, short cd, String customerLast, String amount)
            throws Exception;

    /**
     * Execute payment by customer identifer. Payment is described in clause
     * 2.5.
     * 
     * @param display
     *            Where to display the results, if null results are not
     *            displayed.
     * @param displayData
     *            Any client specific display information, such as servlet
     *            context.
     * @param w
     *            Home warehouse identifier
     * @param d
     *            Home district identifier
     * @param cw
     *            Customer warehouse identifier
     * @param cd
     *            Customer district identifier
     * @param c
     *            Customer identifer.
     * @param amount
     *            Payment amount
     */
    public void payment(Display display, Object displayData, short w, short d,
            short cw, short cd, int c, String amount) throws Exception;

    /**
     * Execute new order. New order is described in clause 2.4.
     * <P>
     * Assumption is that items.length == quanties.length == supplyW.length.
     * 
     * @param display
     *            Where to display the results, if null results are not
     *            displayed.
     * @param displayData
     *            Client specific display information, such as servlet
     *            context.
     * @param w
     *            Warehouse identifier
     * @param d
     *            District identifier
     * @param c
     *            Customer identifier
     * @param items
     *            array of item numbers
     * @param quantities
     *            quanties for each item
     * @param supplyW
     *            Supply warehouse for each item.
     * @throws Exception
     */
    public void newOrder(Display display, Object displayData, short w, short d,
            int c, int[] items, short[] quantities, short[] supplyW)
            throws Exception;

    /**
     * Queue a delivery request. Queuing of delivery requests is described in
     * clause 2.7.2.
     * <P>
     * The implementation of Operations is responsible for managing the FIFO
     * queue of requests, which could be in a flat file, the database or
     * memory etc.
     * 
     * @param display
     *            Where to display the results, if null results are not
     *            displayed.
     * @param displayData
     *            Any client specific display information, such as servlet
     *            context.
     * @param w
     *            Warehouse identifier
     * @param carrier
     *            Carrier identifier
     * @throws Exception
     */
    public void scheduleDelivery(Display display, Object displayData, short w,
            short carrier) throws Exception;

    /**
     * Execute a single delivery from the FIFO queue. Processing a delivery
     * request is described in clause 2.7.4.
     * 
      * @throws Exception
     */
    public void delivery() throws Exception;
    
    /**
     * Release any resources.
     * 
     */
    public void close() throws Exception;

}
