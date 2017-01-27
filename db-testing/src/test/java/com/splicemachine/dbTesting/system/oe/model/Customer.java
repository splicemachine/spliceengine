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
 * An Order Entry customer.
 * <P>
 * Fields map to definition in TPC-C for the CUSTOMER table.
 * The Java names of fields do not include the C_ prefix
 * and are in lower case.
 * <BR>
 * For clarity these fields are renamed in Java
 * <UL>
 * <LI>w_id => warehouse (SQL column C_W_ID)
 * <LI>d_id => district (SQL column C_D_ID)
 * </UL>
 * <BR>
 * The columns that map to an address are extracted out as
 * a Address object with the corresponding Java field address.
 * <BR>
 * All fields have Java bean setters and getters.
 * <BR>
 * Fields that are DECIMAL in the database map to String in Java
 * (rather than BigDecimal) to allow running on J2ME/CDC/Foundation.
 * <P>
 * Primary key maps to {warehouse,district,id}.
 * <P>
 * A Customer object may sparsely populated, when returned from a
 * business transaction it is only guaranteed to contain  the information
 * required to display the result of that transaction.
 * 
 */
public class Customer {
    
    private short warehouse;  
    private short district;
    private int id;
    
    private String first;
    private String middle;
    private String last;   
    private Address address;
    private String phone;
    private Timestamp since;
    private String credit;
    private String credit_lim;
    private String discount;
    private String balance;
    private String ytd_payment;
    private int payment_cnt;
    private int delivery_cnt;
    private String data;
    
    /**
     * Clear all information to allow object re-use.
     */
    public void clear()
    {
        warehouse = district = 0;
        id = 0;     
        first = middle = last = null;
        address = null;     
        phone = null;       
        since = null;       
        credit = credit_lim = discount = null;      
        ytd_payment = null;     
        payment_cnt = delivery_cnt = 0; 
        data = null;
    }
    
    public Address getAddress() {
        return address;
    }
    public void setAddress(Address address) {
        this.address = address;
    }
    public String getBalance() {
        return balance;
    }
    public void setBalance(String balance) {
        this.balance = balance;
    }
    public String getCredit() {
        return credit;
    }
    public void setCredit(String credit) {
        this.credit = credit;
    }
    public String getCredit_lim() {
        return credit_lim;
    }
    public void setCredit_lim(String credit_lim) {
        this.credit_lim = credit_lim;
    }
    public String getData() {
        return data;
    }
    public void setData(String data) {
        this.data = data;
    }
    public int getDelivery_cnt() {
        return delivery_cnt;
    }
    public void setDelivery_cnt(int delivery_cnt) {
        this.delivery_cnt = delivery_cnt;
    }
    public String getDiscount() {
        return discount;
    }
    public void setDiscount(String discount) {
        this.discount = discount;
    }
    public short getDistrict() {
        return district;
    }
    public void setDistrict(short district) {
        this.district = district;
    }
    public String getFirst() {
        return first;
    }
    public void setFirst(String first) {
        this.first = first;
    }
    public int getId() {
        return id;
    }
    public void setId(int id) {
        this.id = id;
    }
    public String getLast() {
        return last;
    }
    public void setLast(String last) {
        this.last = last;
    }
    public String getMiddle() {
        return middle;
    }
    public void setMiddle(String middle) {
        this.middle = middle;
    }
    public int getPayment_cnt() {
        return payment_cnt;
    }
    public void setPayment_cnt(int payment_cnt) {
        this.payment_cnt = payment_cnt;
    }
    public String getPhone() {
        return phone;
    }
    public void setPhone(String phone) {
        this.phone = phone;
    }
    public Timestamp getSince() {
        return since;
    }
    public void setSince(Timestamp since) {
        this.since = since;
    }
    public short getWarehouse() {
        return warehouse;
    }
    public void setWarehouse(short warehouse) {
        this.warehouse = warehouse;
    }
    public String getYtd_payment() {
        return ytd_payment;
    }
    public void setYtd_payment(String ytd_payment) {
        this.ytd_payment = ytd_payment;
    }
    
}

