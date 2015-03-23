package com.splicemachine.derby.impl.db;

import com.splicemachine.constants.SpliceConstants;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Scott Fines
 *         Date: 3/17/15
 */
public class AuthenticationConstants extends SpliceConstants{
    static{
        setParameters(config);
    }
    /**
     * The type of algorithm to use for native encryption.  Optional values are
     *  MD5, SHA-256, and SHA-512 (Default).
     *
     * Defaults to none
     */
    @Parameter public static final String AUTHENTICATION = "splice.authentication";
    @DefaultValue(AUTHENTICATION) public static final String DEFAULT_AUTHENTICATION = "NONE";
    public static String authentication;

    @Parameter public static final String AUTHENTICATION_LDAP_SERVER = "splice.authentication.ldap.server";
    @DefaultValue(AUTHENTICATION_LDAP_SERVER) public static final String DEFAULT_AUTHENTICATION_LDAP_SERVER = "localhost:389";
    public static String authenticationLDAPServer;

    @Parameter public static final String AUTHENTICATION_LDAP_SEARCHAUTHDN = "splice.authentication.ldap.searchAuthDN";
    @DefaultValue(AUTHENTICATION_LDAP_SEARCHAUTHDN) public static final String DEFAULT_AUTHENTICATION_LDAP_SEARCHAUTHDN = "";
    public static String authenticationLDAPSearchAuthDN;

    @Parameter public static final String AUTHENTICATION_LDAP_SEARCHAUTHPW = "splice.authentication.ldap.searchAuthPW";
    @DefaultValue(AUTHENTICATION_LDAP_SEARCHAUTHPW) public static final String DEFAULT_AUTHENTICATION_LDAP_SEARCHAUTHPW = "";
    public static String authenticationLDAPSearchAuthPW;

    @Parameter public static final String AUTHENTICATION_LDAP_SEARCHBASE = "splice.authentication.ldap.searchBase";
    @DefaultValue(AUTHENTICATION_LDAP_SEARCHBASE) public static final String DEFAULT_AUTHENTICATION_LDAP_SEARCHBASE = "";
    public static String authenticationLDAPSearchBase;

    @Parameter public static final String AUTHENTICATION_LDAP_SEARCHFILTER = "splice.authentication.ldap.searchFilter";
    @DefaultValue(AUTHENTICATION_LDAP_SEARCHFILTER) public static final String DEFAULT_AUTHENTICATION_LDAP_SEARCHFILTER = "";
    public static String authenticationLDAPSearchFilter;

    @Parameter public static final String AUTHENTICATION_NATIVE_ALGORITHM = "splice.authentication.native.algorithm";
    @DefaultValue(AUTHENTICATION_NATIVE_ALGORITHM) public static final String DEFAULT_AUTHENTICATION_NATIVE_ALGORITHM = "SHA-512";
    public static String authenticationNativeAlgorithm;

    // Property to force the creation of the native credentials database.
    // Generally, this is only done at the time of the creation of the whole Splice/Derby database.
    // In this particular instance, there are Splice beta customers with AnA disabled and they want to
    // switch to using native AnA.  So we allow a manual override here.  See DB-2088 for more details.
    @Parameter public static final String AUTHENTICATION_NATIVE_CREATE_CREDENTIALS_DATABASE = "splice.authentication.native.create.credentials.database";
    @DefaultValue(AUTHENTICATION_NATIVE_CREATE_CREDENTIALS_DATABASE) public static final boolean DEFAULT_AUTHENTICATION_NATIVE_CREATE_CREDENTIALS_DATABASE = false;
    public static boolean authenticationNativeCreateCredentialsDatabase;

    @Parameter public static final String AUTHENTICATION_CUSTOM_PROVIDER = "splice.authentication.custom.provider";
    @DefaultValue(AUTHENTICATION_CUSTOM_PROVIDER) public static final String DEFAULT_AUTHENTICATION_CUSTOM_PROVIDER = "com.splicemachine.derby.authentication.SpliceUserAuthentication";
    public static String authenticationCustomProvider;

    public static void setParameters(Configuration config){
        authentication = config.get(AUTHENTICATION, DEFAULT_AUTHENTICATION);
        authenticationNativeAlgorithm = config.get(AUTHENTICATION_NATIVE_ALGORITHM, DEFAULT_AUTHENTICATION_NATIVE_ALGORITHM);
        authenticationNativeCreateCredentialsDatabase = config.getBoolean(AUTHENTICATION_NATIVE_CREATE_CREDENTIALS_DATABASE, DEFAULT_AUTHENTICATION_NATIVE_CREATE_CREDENTIALS_DATABASE);

        authenticationCustomProvider = config.get(AUTHENTICATION_CUSTOM_PROVIDER,DEFAULT_AUTHENTICATION_CUSTOM_PROVIDER);

        authenticationLDAPServer = config.get(AUTHENTICATION_LDAP_SERVER,DEFAULT_AUTHENTICATION_LDAP_SERVER);
        authenticationLDAPSearchAuthDN = config.get(AUTHENTICATION_LDAP_SEARCHAUTHDN,DEFAULT_AUTHENTICATION_LDAP_SEARCHAUTHDN);
        authenticationLDAPSearchAuthPW = config.get(AUTHENTICATION_LDAP_SEARCHAUTHPW,DEFAULT_AUTHENTICATION_LDAP_SEARCHAUTHPW);
        authenticationLDAPSearchBase = config.get(AUTHENTICATION_LDAP_SEARCHBASE,DEFAULT_AUTHENTICATION_LDAP_SEARCHBASE);
        authenticationLDAPSearchFilter = config.get(AUTHENTICATION_LDAP_SEARCHFILTER,DEFAULT_AUTHENTICATION_LDAP_SEARCHFILTER);
    }
}
