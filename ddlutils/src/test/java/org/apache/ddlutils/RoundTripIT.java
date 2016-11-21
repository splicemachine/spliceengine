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

package org.apache.ddlutils;

import static org.apache.ddlutils.testutils.TestUtils.DRIVERCLASSNAME;
import static org.apache.ddlutils.testutils.TestUtils.PASSWORD;
import static org.apache.ddlutils.testutils.TestUtils.URL;
import static org.apache.ddlutils.testutils.TestUtils.USERNAME;
import static org.apache.ddlutils.testutils.TestUtils.assertSchemaDropped;
import static org.apache.ddlutils.testutils.TestUtils.countFileLines;
import static org.apache.ddlutils.testutils.TestUtils.countRecords;
import static org.apache.ddlutils.testutils.TestUtils.evaluateScript;
import static org.apache.ddlutils.testutils.TestUtils.getInputFile;
import static org.apache.ddlutils.testutils.TestUtils.getInputFileAsResource;
import static org.apache.ddlutils.testutils.TestUtils.getOutputFile;
import static org.apache.ddlutils.testutils.TestUtils.readFile;
import static org.apache.ddlutils.testutils.TestUtils.setup;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.Connection;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.task.SpliceExportImportSqlToFileCommand;
import org.apache.ddlutils.task.WriteSchemaSqlToFileCommand;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Dump schema, export data, drop schema, create schema, import data.
 * <p/>
 * The flow of these round-trip tests is as follows:
 * <ol>
 *     <li>Set up the database by installing schema(s) and data. If this fails, the whole test fails.<br/>
 *     Also, and validation items can be set up at this time: count the rows in a table, assert it's the same number
 *     as records in an imported csv, etc. This number can then be saved as the expected number of rows.</li>
 *     <li>Use ddlutils to read the database model -- schema, tables, wildcards allowed (%), including
 *     {@link org.apache.ddlutils.model.TableType table types}</li>
 *     <li>Dump a DDL create script to a local file using the database model.</li>
 *     <li>Create export and import scripts to local files using the database model.</li>
 *     <li>Export the data for the database model (to HDFS in real life) with the created export script.</li>
 *     <li>Drop the model in the database and verify it's gone.</li>
 *     <li>Create the schema in the database given the script we created with the database model.</li>
 *     <li>Import the exported data into the new schema.</li>
 *     <li><i>Optional:</i> there may be an additional script to run <i>after</i> data import, like create triggers, for example.</li>
 *     <li>Validate the new model is the same as the old, buy comparing row counts with those done on the old model, for instance,
 *     or something more elaborate</li>
 * </ol>
 */
public class RoundTripIT {


    private static BasicDataSource dataSource;

    @BeforeClass
    public static void beforeClass() throws Exception {
        dataSource = new BasicDataSource();
        dataSource.setDriverClassName(DRIVERCLASSNAME);
        dataSource.setUrl(URL);
        dataSource.setUsername(USERNAME);
        dataSource.setPassword(PASSWORD);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    //============================================================================================

    @Test
    public void testSmallRoundTrip() throws Exception {
        String SCHEMA_NAME = "ROUNDTRIP_SMALL";
        Platform platform = PlatformFactory.createNewPlatformInstance(dataSource);
        Database db = null;
        try (Connection connection = dataSource.getConnection()) {
            try {
                // set up
                setup(getInputFileAsResource("roundtrip_small.sql", getClass()), platform, connection);

                // set up expected counts. Count records in table and in import file. Assert they are equal.
                long expectedCategoryCnt = countRecords("ROUNDTRIP_SMALL.CATEGORY", connection);
                assertEquals("Unexpected count", countFileLines(getInputFileAsResource("small_msdatasample/category.csv",
                                                                                       getClass())),
                             expectedCategoryCnt);
                long expectedCustomerCnt = countRecords("ROUNDTRIP_SMALL.CUSTOMER", connection);
                assertEquals("Unexpected count", countFileLines(getInputFileAsResource("small_msdatasample/customer_iso.csv",
                                                                                       getClass())),
                             expectedCustomerCnt);

                // read model just created
                db = platform.readModelFromDatabase("roundtrip_small", null, SCHEMA_NAME, null);

                // write create script to dump DDL (this is the target file)
                File ddlOutput = getOutputFile("roundtrip_small.sql");

                // write the DDL
                WriteSchemaSqlToFileCommand toSqlFile = new WriteSchemaSqlToFileCommand();
                toSqlFile.setPlatform(platform);
                toSqlFile.setOutputFile(ddlOutput);
                toSqlFile.setAlterDatabase(false);
                toSqlFile.setDoDrops(false);
                toSqlFile.execute(null, db);

                // create write export and import scripts
                SpliceExportImportSqlToFileCommand toExportImport = new SpliceExportImportSqlToFileCommand();
                toExportImport.setPlatform(platform);
                toExportImport.setExportScriptFile(getOutputFile("roundtrip_small_export.sql"));
                toExportImport.setImportScriptFile(getOutputFile("roundtrip_small_import.sql"));
                toExportImport.setExportDirectory(getOutputFile("/export").getCanonicalPath());
                toExportImport.execute(null, db);

                // export data - execute export script
                evaluateScript(getInputFile("/target/roundtrip_small_export.sql"), platform, connection, true);

                // drop model
                platform.dropModel(connection, db, false);
                assertSchemaDropped(SCHEMA_NAME, connection);

                // create model
                assertTrue("File does not exist: '"+ddlOutput+"'", ddlOutput.exists());
                String schemaAsString = readFile(ddlOutput.toPath());
                platform.evaluateBatch(connection, schemaAsString, true);

                // import data - execute import script
                String importStr = readFile(getOutputFile("roundtrip_small_import.sql").toPath());
                platform.evaluateBatch(connection, importStr, true);

                // validate
                assertEquals("Unexpected count!", expectedCategoryCnt, countRecords("ROUNDTRIP_SMALL.CATEGORY", connection));
                assertEquals("Unexpected count!", expectedCustomerCnt, countRecords("ROUNDTRIP_SMALL.CUSTOMER", connection));
            } finally {
                if (db != null) {
                    platform.dropModel(dataSource.getConnection(), db, true);
                }
            }
            assertSchemaDropped(SCHEMA_NAME, connection);
        }
    }

    @Test
    @Ignore("Strange exception creating foreign key: Constraint 'C2_FK' is invalid: there is no unique or primary key " +
        "constraint on table 'ROUNDTRIP_FOREIGNKEY.P' that matches the number and types of the columns in the foreign key.")
    public void roundTripForeignKeys() throws Exception {
        String SCHEMA_NAME = "ROUNDTRIP_FOREIGNKEY";
        Platform platform = PlatformFactory.createNewPlatformInstance(dataSource);
        Database db = null;
        try (Connection connection = dataSource.getConnection()) {
            try {
                // use previously generated create script
                setup(getInputFileAsResource("roundtrip_foreignkey.sql", getClass()), platform, connection);

                // set up expected counts. Count records in table and in import file. Assert they are equal.
                long expectedC1Cnt = countRecords("ROUNDTRIP_FOREIGNKEY.C1", connection);
                assertEquals("Unexpected count", countFileLines(getInputFileAsResource("foreign_key/c1.csv",
                                                                                       getClass())),
                             expectedC1Cnt);
                long expectedPCnt = countRecords("ROUNDTRIP_FOREIGNKEY.P", connection);
                assertEquals("Unexpected count", countFileLines(getInputFileAsResource("foreign_key/p.csv",
                                                                                       getClass())),
                             expectedPCnt);

                // read model just created
                db = platform.readModelFromDatabase("foreign-keys", null, SCHEMA_NAME, null);

                // write create script to dump DDL (this is the target file)
                File ddlOutput = getOutputFile("roundtrip_foreignkey.sql");

                // write the DDL
                WriteSchemaSqlToFileCommand toSqlFile = new WriteSchemaSqlToFileCommand();
                toSqlFile.setPlatform(platform);
                toSqlFile.setOutputFile(ddlOutput);
                toSqlFile.setAlterDatabase(false);
                toSqlFile.setDoDrops(false);
                toSqlFile.execute(null, db);

                // export data - create write export and import scripts
                SpliceExportImportSqlToFileCommand toExportImport = new SpliceExportImportSqlToFileCommand();
                toExportImport.setPlatform(platform);
                toExportImport.setExportScriptFile(getOutputFile("roundtrip_foreignkey_export.sql"));
                toExportImport.setImportScriptFile(getOutputFile("roundtrip_foreignkey_import.sql"));
                toExportImport.setExportDirectory(getOutputFile("/export").getCanonicalPath());
                toExportImport.execute(null, db);

                // export data - execute export script
                evaluateScript(getInputFile("/target/roundtrip_foreignkey_export.sql"), platform, connection, true);

                // drop model
                platform.dropModel(connection, db, false);
                assertSchemaDropped(SCHEMA_NAME, connection);

                // create model
                assertTrue("File does not exist: '"+ddlOutput+"'", ddlOutput.exists());
                String schemaAsString = readFile(ddlOutput.toPath());
                platform.evaluateBatch(connection, schemaAsString, true);

                // import data - execute import script
                String importStr = readFile(getOutputFile("roundtrip_foreignkey_import.sql").toPath());
                platform.evaluateBatch(connection, importStr, true);

                // validate
                assertEquals("Unexpected count!", expectedC1Cnt, countRecords("ROUNDTRIP_FOREIGNKEY.C1", connection));
                assertEquals("Unexpected count!", expectedPCnt, countRecords("ROUNDTRIP_FOREIGNKEY.P", connection));
            } finally {
                if (db != null) {
                    platform.dropModel(dataSource.getConnection(), db, true);
                }
            }
            assertSchemaDropped(SCHEMA_NAME, connection);
        }
    }
}
