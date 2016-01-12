package com.splicemachine.derby.impl.db;

import com.splicemachine.access.api.SConfiguration;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class AuthenticationConfiguration{

    /**
     * The type of algorithm to use for native encryption.  Optional values are
     *  MD5, SHA-256, and SHA-512 (Default).
     *
     * Defaults to none
     */
    public static final String AUTHENTICATION = "splice.authentication";
    private static final String DEFAULT_AUTHENTICATION = "NONE";

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

    public static final SConfiguration.Defaults defaults = new SConfiguration.Defaults(){
        @Override
        public boolean hasLongDefault(String key){
            return false;
        }

        @Override
        public long defaultLongFor(String key){
            return 0;
        }

        @Override
        public boolean hasIntDefault(String key){
            return false;
        }

        @Override
        public int defaultIntFor(String key){
            return 0;
        }

        @Override
        public boolean hasStringDefault(String key){
            switch(key){
                case AUTHENTICATION:
                case AUTHENTICATION_NATIVE_ALGORITHM:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public String defaultStringFor(String key){
            switch(key){
                case AUTHENTICATION: return DEFAULT_AUTHENTICATION;
                case AUTHENTICATION_NATIVE_ALGORITHM: return DEFAULT_AUTHENTICATION_NATIVE_ALGORITHM;
                default:
                    throw new IllegalArgumentException("No Authentication default found for key '"+key+"'");
            }
        }

        @Override
        public boolean defaultBooleanFor(String key){
            switch(key){
                case AUTHENTICATION_NATIVE_CREATE_CREDENTIALS_DATABASE: return DEFAULT_AUTHENTICATION_NATIVE_CREATE_CREDENTIALS_DATABASE;
                default:
                    throw new IllegalArgumentException("No Authentication default found for key '"+key+"'");
            }
        }

        @Override
        public boolean hasBooleanDefault(String key){
            switch(key){
                case AUTHENTICATION_NATIVE_CREATE_CREDENTIALS_DATABASE:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public double defaultDoubleFor(String key){
            throw new IllegalArgumentException("No Authentication default found for key '"+key+"'");
        }

        @Override
        public boolean hasDoubleDefault(String key){
            return false;
        }
    };
}
