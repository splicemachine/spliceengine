package com.splicemachine.db.iapi.reference;

import java.lang.reflect.Field;
import java.util.Set;
import java.util.TreeSet;

public class PropertyHelper {
    // These are the six possible values for collation type if the collation
    // derivation is not NONE. If collation derivation is NONE, then collation
    // type should be ignored. The TERRITORY_BASED collation uses the default
    // collator strength while the four with a colon uses a specific strength.
    public static final String UCS_BASIC_COLLATION =
            "UCS_BASIC";
    public static final String TERRITORY_BASED_COLLATION =
            "TERRITORY_BASED";
    public static final String TERRITORY_BASED_PRIMARY_COLLATION =
            "TERRITORY_BASED:PRIMARY";
    public static final String TERRITORY_BASED_SECONDARY_COLLATION =
            "TERRITORY_BASED:SECONDARY";
    public static final String TERRITORY_BASED_TERTIARY_COLLATION =
            "TERRITORY_BASED:TERTIARY";
    public static final String TERRITORY_BASED_IDENTICAL_COLLATION =
            "TERRITORY_BASED:IDENTICAL";
    // Define a static string for collation derivation NONE
    public static final String COLLATION_NONE =
            "NONE";
    /**
     * The default page size to use for tables that contain a long column.
     **/
    public static final String PAGE_SIZE_DEFAULT_LONG = "32768";
    public static final String AUTHENTICATION_PROVIDER_NATIVE =
            "NATIVE:";
    public static final String AUTHENTICATION_PROVIDER_BUILTIN =
            "BUILTIN";
    public static final String AUTHENTICATION_PROVIDER_LDAP =
            "LDAP";
    public static final String AUTHENTICATION_PROVIDER_KERBEROS =
            "KERBEROS";
    public static final String AUTHENTICATION_SERVER_PARAMETER =
            "derby.authentication.server";
    // this suffix on the NATIVE authentication provider means that
    // database operations should be authenticated locally
    public static final String AUTHENTICATION_PROVIDER_LOCAL_SUFFIX =
            ":LOCAL";
    // when local native authentication is enabled, we store this value for db.authentication.provider
    public static final String AUTHENTICATION_PROVIDER_NATIVE_LOCAL =
            AUTHENTICATION_PROVIDER_NATIVE + AUTHENTICATION_PROVIDER_LOCAL_SUFFIX;
    // Property to force the creation of the native credentials database.
    // Generally, this is only done at the time of the creation of the whole Splice/Derby database.
    // In this particular instance, there are Splice beta customers with AnA disabled and they want to
    // switch to using native AnA.  So we allow a manual override here.  See DB-2088 for more details.
    public static final String AUTHENTICATION_NATIVE_CREATE_CREDENTIALS_DATABASE =
            "derby.authentication.native.create.credentials.database";
    // lifetime (in milliseconds) of a NATIVE password. if <= 0, then the password never expires
    public static final String AUTHENTICATION_NATIVE_PASSWORD_LIFETIME =
            "derby.authentication.native.passwordLifetimeMillis";
    // default lifetime (in milliseconds) of a NATIVE password. FOREVER
    public static final long MILLISECONDS_IN_DAY = 1000L * 60L * 60L * 24L;
    public static final long AUTHENTICATION_NATIVE_PASSWORD_LIFETIME_DEFAULT = 0L;
    // threshhold for raising a warning that a password is about to expire.
    // raise a warning if the remaining password lifetime is less than this proportion of the max lifetime.
    public static final String  AUTHENTICATION_PASSWORD_EXPIRATION_THRESHOLD =
            "derby.authentication.native.passwordLifetimeThreshold";
    public static final double  AUTHENTICATION_PASSWORD_EXPIRATION_THRESHOLD_DEFAULT = 0.125;
    /**
     * Property that specifies the name of the hash algorithm to use with
     * the configurable hash authentication scheme.
     */
    public static final String AUTHENTICATION_BUILTIN_ALGORITHM =
            "derby.authentication.builtin.algorithm";
    /**
     * Default value for db.authentication.builtin.algorithm when creating
     * a new database.
     */
    public static final String AUTHENTICATION_BUILTIN_ALGORITHM_DEFAULT =
            "SHA-256";
    /**
     * Alternative default value for db.authentication.builtin.algorithm if
     * {@link #AUTHENTICATION_BUILTIN_ALGORITHM_DEFAULT} is not available at
     * database creation time.
     */
    public static final String AUTHENTICATION_BUILTIN_ALGORITHM_FALLBACK =
            "SHA-1";
    /**
     * Property that specifies the number of bytes with random salt to use
     * when hashing credentials using the configurable hash authentication
     * scheme.
     */
    public static final String AUTHENTICATION_BUILTIN_SALT_LENGTH =
            "derby.authentication.builtin.saltLength";
    /**
     * The default value for db.authentication.builtin.saltLength.
     */
    public static final int AUTHENTICATION_BUILTIN_SALT_LENGTH_DEFAULT = 16;
    /**
     * Property that specifies the number of times to apply the hash
     * function in the configurable hash authentication scheme.
     */
    public static final String AUTHENTICATION_BUILTIN_ITERATIONS =
            "derby.authentication.builtin.iterations";
    /**
     * Default value for db.authentication.builtin.iterations.
     */
    public static final int AUTHENTICATION_BUILTIN_ITERATIONS_DEFAULT = 1000;
    public static final String NO_ACCESS = "NOACCESS";
    public static final String READ_ONLY_ACCESS = "READONLYACCESS";
    public static final String FULL_ACCESS = "FULLACCESS";

    public static Set<String> getAllProperties() {
        Set<String> propertyList = new TreeSet<>();
        // get configuration fields from class com.splicemachine.db.iapi.reference.Property
        for (Field field : com.splicemachine.db.iapi.reference.Property.class.getFields()) {
            try {
                if(field.getType().equals(String.class))
                    propertyList.add( ((String)field.get(null)) );
            } catch (IllegalAccessException e) {
                // continue
            }
        }
        return propertyList;
    }
}
