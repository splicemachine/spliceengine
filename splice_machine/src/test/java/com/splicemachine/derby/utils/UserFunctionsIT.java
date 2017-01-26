/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.utils;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.ResultSet;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNot.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author alex_stybaev
 */
public class UserFunctionsIT {
    private static final String READ_ONLY_ACCESS = "READONLYACCESS"; //=Property.READ_ONLY_ACCESS
    private static final String FULL_ACCESS = "FULLACCESS";
    private static final String NO_ACCESS = "NOACCESS";
    private static final String READ_ONLY_ACCESS_USERS_PROPERTY = "derby.database.readOnlyAccessUsers";
    private static final String FULL_ACCESS_USERS_PROPERTY = "derby.database.fullAccessUsers";

    public static final String CLASS_NAME = UserFunctionsIT.class.getSimpleName().toUpperCase();

    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    public static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    private static final String EXISTING_USER_NAME = "FOO";

    private static final String EXISTING_USER_NAME_2 = "JDoe";

    private static final String EXISTING_USER_NAME_3 = "GHITA";

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        // TBD
                        classWatcher.prepareStatement("CALL SYSCS_UTIL.SYSCS_CREATE_USER('" + EXISTING_USER_NAME + "', 'bar')").execute();
                        classWatcher.prepareStatement("CALL SYSCS_UTIL.SYSCS_CREATE_USER('" + EXISTING_USER_NAME_2 + "', 'jdoe')").execute();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {

                        classWatcher.closeAll();
                    }
                }
            });

    @AfterClass
    public static void tearDown() throws Exception {
        classWatcher.prepareStatement("CALL SYSCS_UTIL.SYSCS_DROP_USER('" + EXISTING_USER_NAME + "')").execute();
        classWatcher.prepareStatement("CALL SYSCS_UTIL.SYSCS_DROP_USER('" + EXISTING_USER_NAME_2 + "')").execute();
        classWatcher.prepareStatement("CALL SYSCS_UTIL.SYSCS_DROP_USER('" + EXISTING_USER_NAME_3 + "')").execute();
        classWatcher.prepareStatement("DROP SCHEMA " + EXISTING_USER_NAME_3.toUpperCase() +" RESTRICT").execute();
    }

    /**
     * Set user access not by dedicated function but in common way (via setting db property)
     * @throws Exception
     */
    @Test
    public void testSetGetDatabaseAccessProperty() throws Exception {


        String setQuery = String.format("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('%s', '%s')",
                READ_ONLY_ACCESS_USERS_PROPERTY, EXISTING_USER_NAME_2);
        methodWatcher.execute(setQuery);

        String getQuery1 = String.format("values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('%s')",
                READ_ONLY_ACCESS_USERS_PROPERTY);
        try (ResultSet resultSet = methodWatcher.executeQuery(getQuery1)) {
            String result = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(resultSet);
            assertThat(EXISTING_USER_NAME_2 + " must be present in result set!", result, containsString(EXISTING_USER_NAME_2));
        }

        String getQuery2 = String.format("values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('%s')",
                FULL_ACCESS_USERS_PROPERTY);
        try (ResultSet resultSet = methodWatcher.executeQuery(getQuery2)) {
            String result = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(resultSet);
            assertThat(EXISTING_USER_NAME_2 + " must not be present in result set!", result, not(containsString(EXISTING_USER_NAME_2)));
        }
    }

    /**
     * verify that we get the associated schema created when a user is created.
     * @throws Exception
     */

    @Test
    public void verifyUserSchemaCreated() throws Exception {
        String sysUserSchemaQuery = String.format("select count(*) as result from SYS.SYSSCHEMAS where SCHEMANAME='%s'",EXISTING_USER_NAME);
        try (ResultSet resultSet = methodWatcher.executeQuery(sysUserSchemaQuery)) {
            String result = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(resultSet);
            assertThat(EXISTING_USER_NAME_2 + " must  present in result set!", result,
                    is(
                       "RESULT |\n" +
                       "--------\n" +
                       "   1   |"
                      ));
        }

    }

    /**
     * Make sure that we don't fail is there is conflicting schema with the same name as the username
     * we want to preserve the current schema and let the admin resolve that manually
     * @throws Exception
     */

    @Test
    public void createUserWithConflictingSchema() throws Exception {



        // clean and remove the schema
        try{
            methodWatcher.prepareStatement("DROP SCHEMA " + EXISTING_USER_NAME_3.toUpperCase() + " RESTRICT").execute();
        }catch(Exception e){

        }

        String createSchemaQuery = String.format("CREATE SCHEMA %s",EXISTING_USER_NAME_3);
        methodWatcher.prepareStatement(createSchemaQuery).execute();

        // 1. Make sure the schema have been created
        String sysUserSchemaQuery = String.format("select count(*) as result from SYS.SYSSCHEMAS where SCHEMANAME='%s'",EXISTING_USER_NAME_3.toUpperCase());
        try (ResultSet resultSet = methodWatcher.executeQuery(sysUserSchemaQuery)) {
            String result = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(resultSet);
            assertThat(EXISTING_USER_NAME_3 + " must be present in result set!", result,
                    is(
                            "RESULT |\n" +
                                    "--------\n" +
                                    "   1   |"
                    ));
        }

        //2 .create the user
        methodWatcher.prepareStatement("CALL SYSCS_UTIL.SYSCS_CREATE_USER('" + EXISTING_USER_NAME_3 + "', 'bar')").execute();

        // 3 .Make sure the user exist
        String userQuery = String.format("select count(*) as result from (select USERNAME from SYS.SYSUSERS where USERNAME ='%s') AS T",EXISTING_USER_NAME_3);
        try (ResultSet resultSet = methodWatcher.executeQuery(userQuery)) {
            String result = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(resultSet);
            assertThat(EXISTING_USER_NAME_3 + " must be present in result set!", result,
                    is(
                            "RESULT |\n" +
                                    "--------\n" +
                                    "   1   |"
                    ));
        }

        // 4. Make sure we have only one schema at that name
        try (ResultSet resultSet = methodWatcher.executeQuery(sysUserSchemaQuery)) {
            String result = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(resultSet);
            assertThat(EXISTING_USER_NAME_3 + " must be present in result set!", result,
                    is(
                            "RESULT |\n" +
                            "--------\n" +
                            "   1   |"
                    ));
        }

    }
}
