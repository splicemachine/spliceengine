package com.splicemachine.derby.test.framework.tables;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.dbutils.DbUtils;

import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;

public class SpliceCustomerTable extends SpliceTableWatcher {	
	public static final String TABLE_NAME = "CUSTOMER";
	public static final String CREATE_STRING = "(customer_id int, " +
            "cust_last_name varchar(255), " +
            "cust_first_name varchar(255)," +
            "gender_id smallint, " +
            "cust_birthdate timestamp, " +
            "email varchar(255), " +
            "address varchar(255), " +
            "zipcode varchar(10), " +
            "income_id int, " +
            "cust_city_id int, " +
            "age_years int, " +
            "agerange_id int, " +
            "maritalstatus_id int, " +
            "education_id int, " +
            "housingtype_id int, " +
            "householdcount_id int," +
            "plan_id int, " +
            "first_order timestamp, " +
            "last_order timestamp, " +
            "tenure int, " +
            "recency int, " +
            "status_id int)";
	
	public SpliceCustomerTable(String schemaName) {
		this(TABLE_NAME,schemaName);
	}

	public SpliceCustomerTable(String tableName, String schemaName) {
		super(tableName,schemaName,CREATE_STRING);
	}
	
}
