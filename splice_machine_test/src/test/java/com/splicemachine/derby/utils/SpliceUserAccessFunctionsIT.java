package com.splicemachine.derby.utils;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.apache.derby.iapi.reference.Property;
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
     * Set newly created user access to READONLY and check it
     * @throws Exception
     */
    @Test
    public void testSetAndGetUserAccess1() throws Exception {
        String setQuery = String.format("CALL SYSCS_UTIL.SYSCS_SET_USER_ACCESS('%s', '%s')",
                EXISTING_USER_NAME, Property.READ_ONLY_ACCESS);
        methodWatcher.execute(setQuery);

        String getQuery = String.format("values SYSCS_UTIL.SYSCS_GET_USER_ACCESS('%s')", EXISTING_USER_NAME);
        ResultSet resultSet = methodWatcher.executeQuery(getQuery);
        resultSet.next();
        assertThat("User access must be readonly! ", resultSet.getString(1), is(Property.READ_ONLY_ACCESS));
    }

    /**
     * Reset user access to FULLACCESS and recheck it
     * @throws Exception
     */
    @Test
    public void testSetAndGetUserAccess2() throws Exception {
        String setQuery = String.format("CALL SYSCS_UTIL.SYSCS_SET_USER_ACCESS('%s', '%s')",
                EXISTING_USER_NAME, Property.FULL_ACCESS);
        methodWatcher.execute(setQuery);

        String getQuery = String.format("values SYSCS_UTIL.SYSCS_GET_USER_ACCESS('%s')", EXISTING_USER_NAME);
        ResultSet resultSet = methodWatcher.executeQuery(getQuery);
        resultSet.next();
        assertThat("User access must be fullaccess ", resultSet.getString(1), is(Property.FULL_ACCESS));
    }

    /**
     * Set non-existent user access. Error expected as we cannot set access for non-existent ones.
     * @throws Exception
     */
    @Test
    public void testSetUserAccessNonExistentUser() throws Exception {
        String setQuery = String.format("CALL SYSCS_UTIL.SYSCS_SET_USER_ACCESS('NO-SUCH-USER1', '%s')", Property.FULL_ACCESS);
        try (ResultSet resultSet = methodWatcher.executeQuery(setQuery)) {
            fail("Can't set access for non-existent users");
        } catch (Exception ex) {
            assertThat("Proper error code expected", ex.getLocalizedMessage(), is("The user name 'NO-SUCH-USER1' is not valid. "));
        }
    }

    /**
     * Get non-existent user access. NOACCESS is expected for non-existent user
     * @throws Exception
     */
    @Test
    public void testGetUserAccessNonExistentUser() throws Exception {
        String getQuery = "values SYSCS_UTIL.SYSCS_GET_USER_ACCESS('NO-SUCH-USER1')";
        try (ResultSet resultSet = methodWatcher.executeQuery(getQuery)) {
            resultSet.next();
            assertThat("For non-existent users 'NOACCESS' must be returned", resultSet.getString(1), is(Property.NO_ACCESS));
        }
    }

    /**
     * Set user access not by dedicated function but in common way (via setting db property)
     * @throws Exception
     */
    @Test
    public void testSetGetDatabaseAccessProperty() throws Exception {
        String setQuery = String.format("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY('%s', '%s')",
                Property.READ_ONLY_ACCESS_USERS_PROPERTY, EXISTING_USER_NAME_2);
        methodWatcher.execute(setQuery);

        String getQuery1 = String.format("values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('%s')",
                Property.READ_ONLY_ACCESS_USERS_PROPERTY);
        try (ResultSet resultSet = methodWatcher.executeQuery(getQuery1)) {
            String result = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(resultSet);
            assertThat(EXISTING_USER_NAME_2 + " must be present in result set!", result, containsString(EXISTING_USER_NAME_2));
        }

        String getQuery2 = String.format("values SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY('%s')",
                Property.FULL_ACCESS_USERS_PROPERTY);
        try (ResultSet resultSet = methodWatcher.executeQuery(getQuery2)) {
            String result = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(resultSet);
            assertThat(EXISTING_USER_NAME_2 + " must not be present in result set!", result, not(containsString(EXISTING_USER_NAME_2)));
        }
    }
}
