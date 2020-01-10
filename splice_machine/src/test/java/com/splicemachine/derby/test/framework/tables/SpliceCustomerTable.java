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

package com.splicemachine.derby.test.framework.tables;

import com.splicemachine.derby.test.framework.SpliceTableWatcher;

public class SpliceCustomerTable extends SpliceTableWatcher {	
	public static final String TABLE_NAME = "CUSTOMER";
	
	public static final String CREATE_STRING = "(cst_id int, " +
            "cst_last_name varchar(255), " +
            "cst_first_name varchar(255)," +
            "cst_gender_id smallint, " +
            "cst_birthdate timestamp, " +
            "cst_email varchar(255), " +
            "cst_address varchar(255), " +
            "cst_zipcode varchar(10), " +
            "cst_income_id int, " +
            "cst_city_id int, " +
            "cst_age_years int, " +
            "cst_agerange_id int, " +
            "cst_maritalstatus_id int, " +
            "cst_education_id int, " +
            "cst_housingtype_id int, " +
            "cst_householdcount_id int," +
            "cst_plan_id int, " +
            "cst_first_order timestamp, " +
            "cst_last_order timestamp, " +
            "cst_tenure int, " +
            "cst_recency int, " +
            "cst_status_id int)";
	
	public SpliceCustomerTable(String schemaName) {
		this(TABLE_NAME,schemaName);
	}

	public SpliceCustomerTable(String tableName, String schemaName) {
		super(tableName,schemaName,CREATE_STRING);
	}
	
}
