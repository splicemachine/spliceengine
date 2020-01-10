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

import com.splicemachine.dbTesting.system.oe.model.Customer;
import com.splicemachine.dbTesting.system.oe.model.District;
import com.splicemachine.dbTesting.system.oe.model.Order;
import com.splicemachine.dbTesting.system.oe.model.OrderLine;
import com.splicemachine.dbTesting.system.oe.model.Warehouse;

/**
 * Interface to display the results of the business operations.
 * Methods are called by implementations of Operations.
 * There is no requirement for implementations to follow
 * the layout dictated by the TPC-C specification.
 * All the information required by the TPC-C specification
 * for display will be provided through the passed in parameters.
 * <BR>
 * Objects passed in from the data model (Customer etc.) may not
 * be fully populated, but they will contain all the information
 * required for that specific operation.
 * <BR>
 * Any display method must not retain references to any objects
 * it is passed, the caller may be re-using the objects across transactions.
 * <P>
 * DECIMAL values are represented as String objects to allow
 * Order Entry to be run on J2ME/CDC/Foundation which does
 * not support BigDecimal.
 */
public interface Display {

    /**
     * Display the result of a stock level. Stock level terminal i/o is
     * described in clause 2.8.3.
     * 
     * @param displayData
     *            Client specific display information, such as servlet context.
     * @param w
     *            Warehouse (input)
     * @param d
     *            District (input)
     * @param threshold
     *            Threshold (input)
     * @param lowStock
     *            (result)
     * @throws Exception
     *             Error displaying data
     */
    public void displayStockLevel(Object displayData, short w, short d,
            int threshold, int lowStock) throws Exception;

    /**
     * Display the result of an order status. Order status terminal i/o is
     * decribed in clause 2.6.3.
     * 
     * @param displayData
     *            Client specific display information, such as servlet context.
     * @param byName
     *            Executed by name or by identifier.
     * @param customer
     *            Customer for order
     * @param order
     *            Order fetched.
     * @param lineItems Items for the order
     * @throws Exception
     */
    public void displayOrderStatus(Object displayData, boolean byName,
            Customer customer, Order order, OrderLine[] lineItems) throws Exception;
              
    /**
     * Display the result of a payment. Payment terminal i/o
     * is described in clause 2.5.3.
     * @param displayData Client specific display information, such as servlet context.
     * @param amount Amount of payment.
     * @param byName Executed by name or by identifier.
     * @param warehouse Warehouse of payment
     * @param district District of payment
     * @param customer Customer of payment.
     * @throws Exception
     */
    public void displayPayment(Object displayData, String amount,
            boolean byName, Warehouse warehouse, District district,
            Customer customer) throws Exception;

    /**
     * Display the result of a new order. New order terminal i/o
     * is described in clause 2.4.3.
     * May need more parameters.
     * @param displayData Client specific display information, such as servlet context.
     * @param warehouse Warehouse of new order
     * @param district District of new order
     * @param customer Customer of new order
     * @param order The new order
     * @throws Exception
     */
    public void displayNewOrder(Object displayData, Warehouse warehouse,
            District district, Customer customer, Order order) throws Exception;

    /**
     * Display the result of a delivery schedule.
     * 
     * @param displayData Client specific display information, such as servlet context.
     * @param w Warehouse identifier
     * @param carrier Carrier identifier
     * @throws Exception
     */
    public void displayScheduleDelivery(Object displayData, short w,
            short carrier) throws Exception;
}
