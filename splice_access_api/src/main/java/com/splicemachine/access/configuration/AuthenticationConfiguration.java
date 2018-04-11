/*
 * Copyright (c) 2012 - 2018 Splice Machine, Inc.
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

package com.splicemachine.access.configuration;

import com.splicemachine.db.iapi.reference.Property;

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

    public static final String AUTHENTICATION_LDAP_SEARCHAUTH_PASSWORD = "splice.authentication.ldap.searchAuth.password";
    private static final String DEFAULT_AUTHENTICATION_LDAP_SEARCHAUTHPW = "";

    public static final String AUTHENTICATION_LDAP_SEARCHBASE = "splice.authentication.ldap.searchBase";
    private static final String DEFAULT_AUTHENTICATION_LDAP_SEARCHBASE = "";

    public static final String AUTHENTICATION_LDAP_SEARCHFILTER = "splice.authentication.ldap.searchFilter";
    private static final String DEFAULT_AUTHENTICATION_LDAP_SEARCHFILTER = "";

    public static final String AUTHENTICATION_NATIVE_ALGORITHM = "splice.authentication.native.algorithm";
    private static final String DEFAULT_AUTHENTICATION_NATIVE_ALGORITHM = "SHA-512";

    public static final String AUTHORIZATION_SCHEME = "splice.authorization.scheme";
    public static final String DEFAULT_AUTHORIZATION_SCHEME = "NATIVE";

    public static final String RANGER_SERVICE_NAME = "splice.authorization.ranger.service.name";
    public static final String DEFAULT_RANGER_SERVICE_NAME = "splicemachine";

    public static final String SENTRY_POLLING_INTERVAL = "splice.authorization.sentry.polling.interval";
    public static final int DEFAULT_SENTRY_POLLING_INTERVAL = 60;


    // Property to force the creation of the native credentials database.
    // Generally, this is only done at the time of the creation of the whole Splice/Derby database.
    // In this particular instance, there are Splice beta customers with AnA disabled and they want to
    // switch to using native AnA.  So we allow a manual override here.  See DB-2088 for more details.
    public static final String AUTHENTICATION_NATIVE_CREATE_CREDENTIALS_DATABASE = "splice.authentication.native.create.credentials.database";
    private static final boolean DEFAULT_AUTHENTICATION_NATIVE_CREATE_CREDENTIALS_DATABASE = false;

    public static final String AUTHENTICATION_CUSTOM_PROVIDER = "splice.authentication.custom.provider";
    public static final String DEFAULT_AUTHENTICATION_CUSTOM_PROVIDER = "com.splicemachine.derby.authentication.SpliceUserAuthentication";

    public static final String AUTHENTICATION_LDAP_MAPGROUPATTR = "splice.authentication.ldap.mapGroupAttr";
    private static final String DEFAULT_AUTHENTICATION_LDAP_MAPGROUPATTR = "";

    @Override
    public void setDefaults(ConfigurationBuilder builder, ConfigurationSource configurationSource) {
        builder.authentication = configurationSource.getString(AUTHENTICATION, DEFAULT_AUTHENTICATION);
        builder.rangerServiceName = configurationSource.getString(RANGER_SERVICE_NAME,DEFAULT_RANGER_SERVICE_NAME);
        builder.sentryPollingInterval = configurationSource.getInt(SENTRY_POLLING_INTERVAL,DEFAULT_SENTRY_POLLING_INTERVAL);
        if (System.getProperty(AUTHORIZATION_SCHEME) != null)
            builder.authorizationScheme = System.getProperty(AUTHORIZATION_SCHEME);
        else
            builder.authorizationScheme = configurationSource.getString(AUTHORIZATION_SCHEME, DEFAULT_AUTHORIZATION_SCHEME);
        if (builder.authentication.equals(Property.AUTHENTICATION_PROVIDER_LDAP)) {
            builder.authenticationLdapServer = configurationSource.getString(AUTHENTICATION_LDAP_SERVER, DEFAULT_AUTHENTICATION_LDAP_SERVER);
            builder.authenticationLdapSearchauthdn = configurationSource.getString(AUTHENTICATION_LDAP_SEARCHAUTHDN, DEFAULT_AUTHENTICATION_LDAP_SEARCHAUTHDN);
            builder.authenticationLdapSearchauthpw = configurationSource.getString(AUTHENTICATION_LDAP_SEARCHAUTH_PASSWORD, DEFAULT_AUTHENTICATION_LDAP_SEARCHAUTHPW);
            builder.authenticationLdapSearchbase = configurationSource.getString(AUTHENTICATION_LDAP_SEARCHBASE, DEFAULT_AUTHENTICATION_LDAP_SEARCHBASE);
            builder.authenticationLdapSearchfilter = configurationSource.getString(AUTHENTICATION_LDAP_SEARCHFILTER, DEFAULT_AUTHENTICATION_LDAP_SEARCHFILTER);
            builder.authenticationLdapMapGroupAttr = configurationSource.getString(AUTHENTICATION_LDAP_MAPGROUPATTR, DEFAULT_AUTHENTICATION_LDAP_MAPGROUPATTR);
        } else {
            builder.authenticationNativeAlgorithm = configurationSource.getString(AUTHENTICATION_NATIVE_ALGORITHM,
                                                                                  DEFAULT_AUTHENTICATION_NATIVE_ALGORITHM);
            builder.authenticationNativeCreateCredentialsDatabase =
                configurationSource.getBoolean(AUTHENTICATION_NATIVE_CREATE_CREDENTIALS_DATABASE,
                                               DEFAULT_AUTHENTICATION_NATIVE_CREATE_CREDENTIALS_DATABASE);
        }
    }
}
