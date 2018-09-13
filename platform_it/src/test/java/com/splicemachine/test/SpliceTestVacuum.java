package com.splicemachine.test;

import java.sql.*;

public class SpliceTestVacuum {
    public static void main(String[] args) throws Exception {
        String hostname = "localhost";
        int port = 1527;
        String dbUrl =
                "jdbc:splice://" + hostname + ":" + Integer.toString(port) +
                "/splicedb;user=splice;password=admin";
        try {
            //For the JDBC Driver - Use the Apache Derby Client Driver
            Class.forName("com.splicemachine.db.jdbc.ClientDriver");
        } catch (ClassNotFoundException cne){
            cne.printStackTrace();
            return; //exit early if we can't find the driver
        }

        SpliceTestPlatformWait.wait(hostname, port, 180L);

        try(Connection conn = DriverManager.getConnection(dbUrl)) {
            System.out.println("VACUUM: Connected to database.");
            while (true) {
                //Create a statement
                System.out.println("VACUUM: Start vacuum");
                long start = System.currentTimeMillis();
                try (Statement statement = conn.createStatement()) {
                    statement.execute("call syscs_util.vacuum()");
                } catch (Exception e) {
                    System.out.println("VACUUM: Error to run vacuum");
                    e.printStackTrace();
                }
                long finish = System.currentTimeMillis();
                long spent = finish - start;
                long sleep = Math.max(1000L * 10 - spent, 0L);
                System.out.printf(
                    "VACUUM: Finish vacuum, time used: %d ms. Sleep %d ms to do next vacuum.\n",
                    spent, sleep);
                Thread.sleep(sleep);
            }
        }
    }
}
