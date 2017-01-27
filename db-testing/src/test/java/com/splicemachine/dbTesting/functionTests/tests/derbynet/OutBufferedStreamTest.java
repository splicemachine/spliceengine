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

package com.splicemachine.dbTesting.functionTests.tests.derbynet;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.Test;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.SystemPropertyTestSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 *
 * This program tests streaming a blob with db.drda.streamOutBufferSize
 * configuration.
 *
 * When db.drda.streamOutBufferSize is configured,
 * a buffer of configured size is placed at network server just before
 * sending the stream to the client.
 *
 * Buffer size is 131072
 */
public class OutBufferedStreamTest extends BaseJDBCTestCase {

    /**
     * This constructor takes the name of the test.
     *
     * @param name
     */
    public OutBufferedStreamTest(String name) {
        super(name);
    }

    /**
     * Returns the testsuite with a clientServerDecorator and a
     * CleanDatabaseTestSetup with a table. Also sets system property
     * db.drda.streamOutBufferSize=131072.
     *
     * @return the testsuite
     */
    public static Test suite() {
        Properties properties = new Properties();
        properties.setProperty("derby.drda.streamOutBufferSize", "131072");

        Test suite = TestConfiguration.clientServerSuite (OutBufferedStreamTest.class);
        suite = new SystemPropertyTestSetup(suite, properties);

        return new CleanDatabaseTestSetup(suite) {
            /**
             * Creates the table used in the test case.
             *
             * @throws SQLException
             */
            protected void decorateSQL(Statement s) throws SQLException {
                /* Create a table */
                s.execute("create table TEST_TABLE( TEST_COL blob( 65536 ))");
                getConnection().commit();

            }
        };

    }

    /**
     *  This test inserts a blob of length 65536 containing the series 0-255
     *  256 times and then reads the data back and checks that it is correct.
     */
    public void testOutBufferStream() {

        try {
            PreparedStatement insertLobSt = prepareStatement(
                                "insert into TEST_TABLE( TEST_COL ) values(?)");

            insertLobSt.setBinaryStream(1, createOriginalDataInputStream(65536),
                                                                         65536);
            insertLobSt.executeUpdate();
            insertLobSt.close();
            commit();

            PreparedStatement st = prepareStatement(
                                            "select TEST_COL from TEST_TABLE");
            ResultSet rs = st.executeQuery();

            rs.next();

            InputStream is = rs.getBinaryStream(1);

            int[][] expected = new int[256][256];
            int[][] actual = new int[256][256];

            //Build the expected array.
            for (int i = 0; i < 256; i++) {
                for (int j = 0; j < 256; j++) {
                    expected[i][j] = j;
                }
            }

            //Read data from the lob and build array.
            for (int i = 0; i < 256; i++) {
                for (int j = 0; j < 256; j++) {
                    actual[i][j] = is.read();
                }

            }

            //Assert that the arrays are equal.
            for (int i = 0; i < 256; i++) {
                for (int j = 0; j < 256; j++) {
                    assertEquals("Not correct: Line " + i + " pos " + j,
                                 expected[i][j], actual[i][j]);
                }
            }

            is.close();
            rs.close();
            st.close();
            commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Build a ByteArrayInputStream of the given length, with values
     * from 0 t0 255 so they will fit into a byte.
     */
    private static ByteArrayInputStream createOriginalDataInputStream(int length) {

        byte[] originalValue = new byte[length];

        for (int i = 0; i < originalValue.length; i++) {
            originalValue[i] = (byte) (i % 256);
        }

        return new ByteArrayInputStream(originalValue);

    }
}