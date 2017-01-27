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
package com.splicemachine.dbTesting.system.oe.test;

import java.util.HashMap;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.system.oe.client.Display;
import com.splicemachine.dbTesting.system.oe.client.Operations;
import com.splicemachine.dbTesting.system.oe.client.Submitter;
import com.splicemachine.dbTesting.system.oe.direct.Standard;
import com.splicemachine.dbTesting.system.oe.model.Customer;
import com.splicemachine.dbTesting.system.oe.model.District;
import com.splicemachine.dbTesting.system.oe.model.Order;
import com.splicemachine.dbTesting.system.oe.model.OrderLine;
import com.splicemachine.dbTesting.system.oe.model.Warehouse;
import com.splicemachine.dbTesting.system.oe.util.OERandom;

/**
 * Test an implementation of Operations.
 * Currently just tests the setup but as more
 * code is added the implemetations of the transactions
 * will be added.
 */
public class OperationsTester extends BaseJDBCTestCase implements Display {

    private Operations ops;
    private OERandom rand;
    private final short w = 1;
    
    public OperationsTester(String name) {
        super(name);
        
    }
    
    protected void setUp() throws Exception 
    {
        ops = new Standard(getConnection());
        rand = Submitter.getRuntimeRandom(getConnection());
    }
    
    protected void tearDown() throws Exception
    {
        ops.close();
        super.tearDown();
    }
    
    public void testStockLevel() throws Exception
    {     
        // Check a null display is handled
        ops.stockLevel(null, null,
                w, rand.district(), rand.threshold());
        
        for (int i = 0; i < 20; i++) {
           
            short d = rand.district();
            int threshold = rand.threshold();
            
            HashMap inputData = new HashMap();
            inputData.put("d", new Short(d));
            inputData.put("threshold", new Integer(threshold));
            
            ops.stockLevel(this, inputData,
                    w, d, threshold);
            
            // Ensures the Display object read it.
            assertTrue(inputData.isEmpty());
        }
    }
    
    /**
     * Execute a number of order-status transactions
     * by name and identifier. Also check the implementation
     * accepts a null display.
     * @throws Exception
     */
    public void testOrderStatus() throws Exception
    {     
        // By identifier
        ops.orderStatus(null, null,
                w, rand.district(), rand.NURand1023());
        for (int i = 0; i < 50; i++) {
            
            short d = rand.district();
            int c = rand.NURand1023();
            
            HashMap inputData = new HashMap();
            inputData.put("d", new Short(d));
            inputData.put("c", new Integer(c));

            ops.orderStatus(this, inputData, w, d, c);
            // Ensures the Display object read it.
            assertTrue(inputData.isEmpty());
        }
        
        // By name 
        ops.orderStatus(null, null,
                w, rand.district(), rand.randomCLast());
        for (int i = 0; i < 50; i++)
        {
            short d = rand.district();
            String customerLast = rand.randomCLast();
            
            HashMap inputData = new HashMap();
            inputData.put("d", new Short(d));
            inputData.put("customerLast", customerLast);

            ops.orderStatus(this, inputData, w, d, customerLast);
            // Ensures the Display object read it.
            assertTrue(inputData.isEmpty());
            
        }
    }
    public void testPayment() throws Exception
    {       
        //  With no display
        ops.payment(null, null, w, rand.district(),
                w, rand.district(), rand.randomCLast(), rand.payment().toString());
        
        for (int i = 0; i < 50; i++) {
            ops.payment(this, null, w, rand.district(),
                    w, rand.district(), rand.randomCLast(), rand.payment().toString());
        }  
        
        // With no display
        ops.payment(null, null, w, rand.district(),
                w, rand.district(), rand.NURand1023(), rand.payment().toString());

        for (int i = 0; i < 50; i++) {
            
            ops.payment(this, null, w, rand.district(),
                    w, rand.district(), rand.NURand1023(), rand.payment().toString());
        }
    }
    public void testNewOrder() throws Exception
    {
        for (int x = 0; x < 50; x++)
        {
            int itemCount = rand.randomInt(5, 15);
            int[] items = new int[itemCount];
            short[] quantities = new short[itemCount];
            short[] supplyW = new short[itemCount];
            
            // rollback 1% of the transactions
            boolean willFail = rand.randomInt(1, 100) == 1;

            for (int i = 0 ; i < itemCount; i++) {
                if (willFail && (i == (itemCount - 1)))
                    items[i] = 500000; // some invalid value
                else
                    items[i] = rand.NURand8191();

                quantities[i] = (short) rand.randomInt(1, 10);
                supplyW[i] = w;
            }

            ops.newOrder(this, null, w, rand.district(),
                rand.NURand1023(),  items, quantities, supplyW);
            
        }
    }
    public void testScheduleDelivery() throws Exception
    {
        for (int i = 0; i < 50; i++)
            ops.scheduleDelivery(this, null, w, rand.carrier());
    }
    public void testDelivery() throws Exception
    {
        // Ensure there are some schedule deliveries
        testScheduleDelivery();
        for (int i = 0; i < 50; i++)
            ops.delivery();
    }

    public void displayStockLevel(Object displayData, short w, short d, int threshold, int lowStock) throws Exception {
        
        // Submitter does not fill this in.
        if (displayData == null)
            return;
        
        HashMap inputData = (HashMap) displayData;
        assertEquals("sl:w", this.w, w);
        assertEquals("sl:d", ((Short) inputData.get("d")).shortValue(), d);
        assertEquals("sl:threshold", ((Integer) inputData.get("threshold")).intValue(), threshold);
        assertTrue("sl:low stock", lowStock >= 0);
        
        // Clear it to inform the caller that it was read.
        inputData.clear();
    }

    public void displayOrderStatus(Object displayData, boolean byName, Customer customer, Order order, OrderLine[] lineItems) throws Exception {
        
        // Submitter does not fill this in.
        if (displayData == null)
            return;
        
        HashMap inputData = (HashMap) displayData;
        assertEquals("os:w", this.w, customer.getWarehouse());
        assertEquals("os:d", ((Short) inputData.get("d")).shortValue(), customer.getDistrict());
        
        if (byName)
        {
            assertNotNull(inputData.get("customerLast"));
        }
        else
        {
            assertNull(inputData.get("customerLast"));
        }
        
        // Clear it to inform the caller that it was read.
        inputData.clear();
    }

    public void displayPayment(Object displayData, String amount, boolean byName, Warehouse warehouse, District district, Customer customer) throws Exception {
        // TODO Auto-generated method stub
        
    }

    public void displayNewOrder(Object displayData, Warehouse warehouse, District district, Customer customer, Order order) throws Exception {
        // TODO Auto-generated method stub
        
    }

    public void displayScheduleDelivery(Object displayData, short w, short carrier) throws Exception {
        // TODO Auto-generated method stub
        
    }
    
    /**
     * Test submitting transactions through Submitter,
     * as individual transactions and as a block.
     * @throws Exception
     */
    public void testSubmitter() throws Exception
    {
        Submitter submitter = new Submitter(this, this.ops, this.rand,
                (short) 1);
        
        int tranCount = 37;
        for (int i = 0; i < tranCount; i++)
        {
            submitter.runTransaction(null);
        }
        
        int tranCount2 = 47;
        submitter.runTransactions(null, tranCount2);
        
        int[] executeCounts = submitter.getTransactionCount();
        int totalTran = 0;
        for (int i = 0; i < executeCounts.length; i++)
            totalTran += executeCounts[i];
        
        assertEquals("Mismatch on Submitter transaction count",
                tranCount + tranCount2, totalTran);
        
    }
}
