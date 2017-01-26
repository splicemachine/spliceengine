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
package com.splicemachine.dbTesting.system.oe.model;

import java.sql.Timestamp;

/**
 * An Order Entry order line item.
 * <P>
 * Fields map to definition in TPC-C for the ORDERLINE table.
 * The Java names of fields do not include the OL_ prefix
 * and are in lower case.
 * <BR>
 * All fields have Java bean setters and getters.
 * <BR>
 * Fields that are DECIMAL in the database map to String in Java
 * (rather than BigDecimal) to allow running on J2ME/CDC/Foundation.
 * <P>
 * Primary key maps to {Order,id}, it is assumed that an OrderLine object
 * exists in the context of an Order object, thus the columns
 * {OL_O_ID, OL_D_ID, OL_W_ID}  are not represented in this class.
 * 
 * <P>
 * An OrderLine object may sparsely populated, when returned from a
 * business transaction it is only guaranteed to contain  the information
 * required to display the result of that transaction.
 */
public class OrderLine {
    /**
     * Line item order number.
     */
    private short number;
    /**
     * ITEM number.
     */
    private int i_id;
    private short supply_w_id;
    private Timestamp delivery_d;
    private short quantity;
    private String amount;
    private String dist_info;

    public String getAmount() {
        return amount;
    }
    public void setAmount(String amount) {
        this.amount = amount;
    }
    public Timestamp getDelivery_d() {
        return delivery_d;
    }
    public void setDelivery_d(Timestamp delivery_d) {
        this.delivery_d = delivery_d;
    }
    public String getDist_info() {
        return dist_info;
    }
    public void setDist_info(String dist_info) {
        this.dist_info = dist_info;
    }
    public int getI_id() {
        return i_id;
    }
    public void setI_id(int i_id) {
        this.i_id = i_id;
    }
    public short getNumber() {
        return number;
    }
    public void setNumber(short number) {
        this.number = number;
    }
    public short getQuantity() {
        return quantity;
    }
    public void setQuantity(short quantity) {
        this.quantity = quantity;
    }
    public short getSupply_w_id() {
        return supply_w_id;
    }
    public void setSupply_w_id(short supply_w_id) {
        this.supply_w_id = supply_w_id;
    }
}
