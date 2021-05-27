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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.reference;

import java.util.Arrays;
import java.util.stream.Stream;

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


    public static Stream<String> getAllReferenceProperties() {
        return Arrays.stream(com.splicemachine.db.iapi.reference.Property.class.getFields()).map(
                field -> {
                    if (!field.getType().equals(String.class)) return null;
                    try {
                        return ((String)field.get(null));
                    } catch (IllegalAccessException e) {
                        return null;
                    }
                } ).filter( f -> f != null );
    }

    /**
     * @return  all properties defined in reference.Property + (new) GlobalDBProperties
     */
    public static Stream<GlobalDBProperties.PropertyType> getAllProperties() {
        Stream<GlobalDBProperties.PropertyType> s1 = PropertyHelper.getAllReferenceProperties()
                .map(s -> new GlobalDBProperties.PropertyType(s, "", null));

        return Stream.concat(s1, GlobalDBProperties.getAll());
    }
}
