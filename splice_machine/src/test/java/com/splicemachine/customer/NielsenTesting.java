package com.splicemachine.customer;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.log4j.Logger;

public class NielsenTesting {

    public static String testInternalConnection() throws Exception {
        Connection mconn = DriverManager
                .getConnection("jdbc:default:connection");
        String lv_ref_val = null;
        try {
            if (mconn != null) {
                lv_ref_val = "Got an internal connection";
            }
            else {
                lv_ref_val = "Cannot get an internal connection";
            }
        } finally {
            if (mconn != null) {
                mconn.close();
            }
        }
        return lv_ref_val;
    }
}
