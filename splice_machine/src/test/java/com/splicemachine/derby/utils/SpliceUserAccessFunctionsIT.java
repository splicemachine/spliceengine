/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
public class SpliceUserAccessFunctionsIT {
    private static final String READ_ONLY_ACCESS = "READONLYACCESS"; //=Property.READ_ONLY_ACCESS
    private static final String FULL_ACCESS = "FULLACCESS";
    private static final String NO_ACCESS = "NOACCESS";
    private static final String READ_ONLY_ACCESS_USERS_PROPERTY = "derby.database.readOnlyAccessUsers";
    private static final String FULL_ACCESS_USERS_PROPERTY = "derby.database.fullAccessUsers";

    public static final String CLASS_NAME = SpliceUserAccessFunctionsIT.class.getSimpleName().toUpperCase();

    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    public static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    private static final String EXISTING_USER_NAME = "FOO";

    private static final String EXISTING_USER_NAME_2 = "JDoe";

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
}
