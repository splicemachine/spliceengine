package com.splicemachine.homeless;

import org.apache.commons.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TestUtils {
    public static String getBaseDirectory() {
        String dir = System.getProperty("user.dir");
        if(!dir.endsWith("structured_derby"))
            dir = dir+"/structured_derby";
        return dir+"/src/test/resources/";
    }

    public static void executeSqlFile(Connection conn, String fileSuffix){

        String sqlStatementStrings = null;
        try {
            sqlStatementStrings = IOUtils.toString(new FileInputStream( getBaseDirectory() + fileSuffix));
        } catch (IOException e) {
            throw new RuntimeException("Unable to open file " + fileSuffix, e);
        }

        for( String s : sqlStatementStrings.split(";")){
            Statement stmt = null;
            try {
                stmt = conn.createStatement();
                stmt.execute(s);
                conn.commit();
            } catch (SQLException e) {
                throw new RuntimeException("Error loading SQL file at path : " + fileSuffix, e);
            }
        }
    }
}
