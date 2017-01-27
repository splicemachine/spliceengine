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
 * An Order Entry order.
 * <P>
 * Fields map to definition in TPC-C for the ORDER table.
 * The Java names of fields do not include the O_ prefix
 * and are in lower case.
 * For clarity these fields are renamed in Java
 * <UL>
 * <LI>w_id => warehouse (SQL column O_W_ID)
 * <LI>d_id => district (SQL column O_D_ID)
 * <LI>c_id => customer (SQL column O_C_ID)
 * </UL>
 * <BR>
 * The columns that map to an address are extracted out as
 * a Address object with the corresponding Java field address.
 * <BR>
 * All fields have Java bean setters and getters.
 * <P>
 * Primary key maps to {warehouse,district,id}.
 * 
 * <P>
 * An Order object may sparsely populated, when returned from a
 * business transaction it is only guaranteed to contain  the information
 * required to display the result of that transaction.
 */
public class Order {
    private int id;
    private short district;
    private short warehouse;
    private int customer;
    private Timestamp entry_d;
    private Integer carrier_id; // JDBC maps SMALLINT to java.lang.Integer
    private int ol_cnt;
    private boolean all_local;
    
    /**
     * Clear all information to allow object re-use.
     */
    public void clear()
    {
        id = 0;
        district = warehouse = 0;
        customer = 0;
        entry_d = null;
        carrier_id = null;
        ol_cnt = 0;
        all_local = false;
    }

    public boolean isAll_local() {
        return all_local;
    }
    public void setAll_local(boolean all_local) {
        this.all_local = all_local;
    }
    public Integer getCarrier_id() {
        return carrier_id;
    }
    public void setCarrier_id(Integer carrier_id) {
        this.carrier_id = carrier_id;
    }
    public int getCustomer() {
        return customer;
    }
    public void setCustomer(int customer) {
        this.customer = customer;
    }
    public short getDistrict() {
        return district;
    }
    public void setDistrict(short district) {
        this.district = district;
    }
    public Timestamp getEntry_d() {
        return entry_d;
    }
    public void setEntry_d(Timestamp entry_d) {
        this.entry_d = entry_d;
    }
    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public int getOl_cnt() {
        return ol_cnt;
    }
    public void setOl_cnt(int ol_cnt) {
        this.ol_cnt = ol_cnt;
    }
    public short getWarehouse() {
        return warehouse;
    }
    public void setWarehouse(short warehouse) {
        this.warehouse = warehouse;
    }
}
