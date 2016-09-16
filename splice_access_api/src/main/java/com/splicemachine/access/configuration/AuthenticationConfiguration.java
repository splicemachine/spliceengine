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

package com.splicemachine.access.configuration;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class AuthenticationConfiguration implements ConfigurationDefault {

    /**
     * The type of algorithm to use for native encryption.  Optional values are
     *  MD5, SHA-256, and SHA-512 (Default).
     *
     * Defaults to none
     */
    public static final String AUTHENTICATION = "splice.authentication";
    private static final String DEFAULT_AUTHENTICATION = "NATIVE";

    public static final String AUTHENTICATION_LDAP_SERVER = "splice.authentication.ldap.server";
    private static final String DEFAULT_AUTHENTICATION_LDAP_SERVER = "localhost:389";

    public static final String AUTHENTICATION_LDAP_SEARCHAUTHDN = "splice.authentication.ldap.searchAuthDN";
    private static final String DEFAULT_AUTHENTICATION_LDAP_SEARCHAUTHDN = "";

    public static final String AUTHENTICATION_LDAP_SEARCHAUTHPW = "splice.authentication.ldap.searchAuthPW";
    private static final String DEFAULT_AUTHENTICATION_LDAP_SEARCHAUTHPW = "";

    public static final String AUTHENTICATION_LDAP_SEARCHBASE = "splice.authentication.ldap.searchBase";
    private static final String DEFAULT_AUTHENTICATION_LDAP_SEARCHBASE = "";

    public static final String AUTHENTICATION_LDAP_SEARCHFILTER = "splice.authentication.ldap.searchFilter";
    private static final String DEFAULT_AUTHENTICATION_LDAP_SEARCHFILTER = "";

    public static final String AUTHENTICATION_NATIVE_ALGORITHM = "splice.authentication.native.algorithm";
    private static final String DEFAULT_AUTHENTICATION_NATIVE_ALGORITHM = "SHA-512";

    // Property to force the creation of the native credentials database.
    // Generally, this is only done at the time of the creation of the whole Splice/Derby database.
    // In this particular instance, there are Splice beta customers with AnA disabled and they want to
    // switch to using native AnA.  So we allow a manual override here.  See DB-2088 for more details.
    public static final String AUTHENTICATION_NATIVE_CREATE_CREDENTIALS_DATABASE = "splice.authentication.native.create.credentials.database";
    private static final boolean DEFAULT_AUTHENTICATION_NATIVE_CREATE_CREDENTIALS_DATABASE = true;

    public static final String AUTHENTICATION_CUSTOM_PROVIDER = "splice.authentication.custom.provider";
    public static final String DEFAULT_AUTHENTICATION_CUSTOM_PROVIDER = "com.splicemachine.derby.authentication.SpliceUserAuthentication";

    @Override
    public void setDefaults(ConfigurationBuilder builder, ConfigurationSource configurationSource) {
        builder.authentication = configurationSource.getString(AUTHENTICATION, DEFAULT_AUTHENTICATION);

        if (builder.authentication.equals("LDAP")) {
            builder.authenticationLdapServer = configurationSource.getString(AUTHENTICATION_LDAP_SERVER, DEFAULT_AUTHENTICATION_LDAP_SERVER);
            builder.authenticationLdapSearchauthdn = configurationSource.getString(AUTHENTICATION_LDAP_SEARCHAUTHDN, DEFAULT_AUTHENTICATION_LDAP_SEARCHAUTHDN);
            builder.authenticationLdapSearchauthpw = configurationSource.getString(AUTHENTICATION_LDAP_SEARCHAUTHPW, DEFAULT_AUTHENTICATION_LDAP_SEARCHAUTHPW);
            builder.authenticationLdapSearchbase = configurationSource.getString(AUTHENTICATION_LDAP_SEARCHBASE, DEFAULT_AUTHENTICATION_LDAP_SEARCHBASE);
            builder.authenticationLdapSearchfilter = configurationSource.getString(AUTHENTICATION_LDAP_SEARCHFILTER, DEFAULT_AUTHENTICATION_LDAP_SEARCHFILTER);
        } else {
            builder.authenticationNativeAlgorithm = configurationSource.getString(AUTHENTICATION_NATIVE_ALGORITHM,
                                                                                  DEFAULT_AUTHENTICATION_NATIVE_ALGORITHM);
            builder.authenticationNativeCreateCredentialsDatabase =
                configurationSource.getBoolean(AUTHENTICATION_NATIVE_CREATE_CREDENTIALS_DATABASE,
                                               DEFAULT_AUTHENTICATION_NATIVE_CREATE_CREDENTIALS_DATABASE);
        }
    }
}
