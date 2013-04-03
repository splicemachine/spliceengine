package com.splicemachine.homeless;

import org.apache.commons.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TestUtils {

    public static URL getClasspathResource(String path){
        return TestUtils.class.getClassLoader().getResource(path);
    }

    public static void executeSqlFile(Connection conn, String fileSuffix){

        String sqlStatementStrings = null;
        try {

            URL pathToFile = getClasspathResource(fileSuffix);
            sqlStatementStrings = IOUtils.toString(pathToFile);

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
