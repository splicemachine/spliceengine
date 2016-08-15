/*
 * ddlUtils is a subproject of the Apache DB project, and is licensed under
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.ddlutils.testutils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Schema;
import org.apache.ddlutils.model.Table;
import org.apache.ddlutils.model.TableType;

/**
 * Utility methods for testing ddlutils.
 */
public class TestUtils {

    /**
     * Assert that the given schema name does not exist in teh database.
     * @param schemaName complete schema name or partial (eg: "MY_SCHEM&'0.
     * @param compareOperator operator string used in criteria. Either "=", for exact match, or "LIKE", for partial.
     * @param connection the connection to use.
     * @throws SQLException
     */
    public static void assertSchemaDropped(String schemaName, String compareOperator, Connection connection) throws SQLException {
        String query = String.format("select SCHEMANAME from SYS.SYSSCHEMAS WHERE SCHEMANAME %s '%s'", compareOperator, schemaName);
        try (Statement st = connection.createStatement()) {
            try (ResultSet rs = st.executeQuery(query)) {
                if (rs.next()) {
                    fail("Expected not to find schema '"+schemaName+"'.  Not dropped.");
                }
            }
        }
    }

    /**
     * Assert the count of the regex <code>pattern</code> in <code>searchString</code> match the <code>expected</code>.
     * @param pattern literal string or regex pattern.
     * @param searchString the string to search.
     * @param expected number of expected matches.
     */
    public static void assertCountString(String pattern, String searchString, int expected) {
        Pattern p = Pattern.compile(pattern);
        Matcher m = p.matcher(searchString);
        int count = 0;
        while (m.find()){
            count +=1;
        }
        assertEquals("String count wrong for '"+pattern+"' in:\n"+searchString, expected, count);
    }

    /**
     * Count the all tables in the given schema of the given type and assert it's the same as expected.
     * @param schema schema containing tables.
     * @param expected expected number of tables.
     * @param type {@link TableType type} of the table.
     */
    public static void assertCountTableType(Schema schema, int expected, TableType type) {
        assertNotNull("Schema is null.", schema);
        int actual = 0;
        for (Table table : schema.getTables()) {
            if (table.getType().equals(type)) ++actual;
        }
        assertEquals("Wrong count for tables of type '"+type+"' in schema '"+schema.getSchemaName()+"'", expected, actual);
    }

    /**
     * Count the tables in the given model of the given type and assert it's the same as expected.
     * @param model database model containing schemas which contain tables.
     * @param expected expected number of tables.
     * @param type {@link TableType type} of the table.
     */
    public static void assertCountTableType(Database model, int expected, TableType type) {
        assertNotNull("Database model is null.", model);
        int actual = 0;
        for (Schema schema : model.getSchemas()) {
            for (Table table : schema.getTables()) {
                if (table.getType().equals(type)) ++actual;
            }
        }
        assertEquals("Wrong count for tables of type '"+type+"' in model '"+model.getName()+"'", expected, actual);
    }

    /**
     * Compare the contents of two files ignoring whitespace.
     * @param expected the expected base file name (a resource).
     * @param actual the actual base file name (in the target directory).
     * @throws IOException
     */
    public static void assertFilesCompare(String expected, String actual, Class clazz) throws IOException {
        // expected is in resource dir
        File expectedFile = getInputFile(expected, clazz);
        assertTrue("File does not exist: '"+expectedFile+"'", expectedFile.exists());
        // remove all whitespace because line separators are throwing off comparison
        String expectedString = readFile(expectedFile.toPath()).replaceAll("\\s+","");

        // Actual file is in target dir
        File actualFile = getOutputFile(actual);
        assertTrue("File does not exist: '"+actualFile+"'", actualFile.exists());
        // remove all whitespace because line separators are throwing off comparison
        String actualString = readFile(actualFile.toPath()).replaceAll("\\s+","");

        assertEquals("Files did not compare.", expectedString, actualString);
    }

    /**
     * Create the entities, schema and tables, using the contents of the SQL script.
     * @param fileName base file name of the SQL script (a resource).
     * @param platform the database specific platform.
     * @param connection connection to use.
     * @throws Exception
     */
    public static void createSchema(String fileName, Platform platform, Connection connection) throws Exception {
        File schemaFile = getInputFile(fileName, platform.getClass());
        assertTrue("File does not exist: '"+schemaFile+"'", schemaFile.exists());
        String schemaAsString = readFile(schemaFile.toPath());
        platform.evaluateBatch(connection, schemaAsString, true);
    }

    /**
     * Find or create the file by the given base name in teh project target directory.
     * @param fileName base name of file.
     * @return the file under the project target directory.
     */
    public static File getOutputFile(String fileName) {
        return new File(System.getProperty("user.dir") + "/target/" + fileName);
    }

    /**
     * Look up <code>fileName</code> as a resource, create a file and return it.
     * @param fileName base name of the resource.
     * @return existing file.
     * @throws IOException if the resource fails to be found.
     */
    private static File getInputFile(String fileName, Class clazz) throws IOException {
        ClassLoader classLoader = clazz.getClassLoader();
        URL fileURL = classLoader.getResource(fileName);
        if (fileURL == null) throw new IOException();
        return new File(fileURL.getFile());
    }

    /**
     * Stringify the content of the test file represented by <code>path</code>.
     * @param path test file path
     * @return string content of teh file, unchanged.
     * @throws IOException
     */
    private static String readFile(Path path) throws IOException {
        byte[] encoded = Files.readAllBytes(path);
        return new String(encoded, Charset.defaultCharset());
    }
}
