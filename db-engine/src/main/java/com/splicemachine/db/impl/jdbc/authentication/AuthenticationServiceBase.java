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

package com.splicemachine.db.impl.jdbc.authentication;

import com.google.common.base.Splitter;
import com.splicemachine.db.authentication.UserAuthenticator;
import com.splicemachine.db.catalog.SystemProcedures;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.jdbc.AuthenticationService;
import com.splicemachine.db.iapi.reference.*;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.daemon.Serviceable;
import com.splicemachine.db.iapi.services.monitor.ModuleControl;
import com.splicemachine.db.iapi.services.monitor.ModuleSupportable;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.property.PropertyFactory;
import com.splicemachine.db.iapi.services.property.PropertySetCallback;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.store.access.AccessFactory;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.util.StringUtil;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.*;

/**
 * <p>
 * This is the authentication service base class.
 * </p>

 * <p>
 * There can be 1 Authentication Service for the whole Derby
 * system and/or 1 authentication per database.
 * In a near future, we intend to allow multiple authentication services
 * per system and/or per database.
 * </p>
 *
 * <p>
 * It should be extended by the specialized authentication services.
 * </p>
 *
 * <p><strong>IMPORTANT NOTE:</strong></p>
 *
 * <p>
 * User passwords are hashed using a message digest algorithm
 * if they're stored in the database. They are not hashed
 * if they were defined at the system level.
 * </p>
 *
 * <p>
 * The passwords can be hashed using two different schemes:
 * </p>
 *
 * <ul>
 * <li>The SHA-1 authentication scheme, which was the only available scheme
 * in Derby 10.5 and earlier. This scheme uses the SHA-1 message digest
 * algorithm.</li>
 * <li>The configurable hash authentication scheme, which allows the users to
 * specify which message digest algorithm to use.</li>
 * </ul>
 *
 * <p>
 * In order to use the configurable hash authentication scheme, the users have
 * to set the {@code db.authentication.builtin.algorithm} property (on
 * system level or database level) to the name of an algorithm that's available
 * in one of the security providers registered on the system. If this property
 * is not set, or if it's set to NULL or an empty string, the SHA-1
 * authentication scheme is used.
 * </p>
 *
 * <p>
 * Which scheme to use is decided when a password is about to be stored in the
 * database. One database may therefore contain passwords stored using
 * different schemes. In order to determine which scheme to use when comparing
 * a user's credentials with those stored in the database, the stored password
 * is prefixed with an identifier that tells which scheme is being used.
 * Passwords stored using the SHA-1 authentication scheme are prefixed with
 * {@link PasswordHasher#ID_PATTERN_SHA1_SCHEME}. Passwords that are stored using the
 * configurable hash authentication scheme are prefixed with
 * {@link PasswordHasher#ID_PATTERN_CONFIGURABLE_HASH_SCHEME} and suffixed with the name of
 * the message digest algorithm.
 * </p>
 */
public abstract class AuthenticationServiceBase
	implements AuthenticationService, ModuleControl, ModuleSupportable, PropertySetCallback {

	protected UserAuthenticator authenticationScheme;

	// required to retrieve service properties
	private AccessFactory store;

	/**
		Trace flag to trace authentication operations
	*/
	public static final String AuthenticationTrace =
						SanityManager.DEBUG ? "AuthenticationTrace" : null;

    /**
        Userid with Strong password substitute DRDA security mechanism
    */
    protected static final int SECMEC_USRSSBPWD = 8;

	private static final String IMPERSONATION_ENABLED = "derby.authentication.impersonation.enabled";
	private static final String IMPERSONATION_USERS = "derby.authentication.impersonation.users";
	private static final String LDAP_GROUP_ATTR = "derby.authentication.ldap.mapGroupAttr";

	//
	// constructor
	//
	public AuthenticationServiceBase() {
	}

	protected String impersonate(String userName, String proxyUser) {
		if (!Boolean.parseBoolean(getProperty(IMPERSONATION_ENABLED))) {
			 return null;
		}

		String allPermissions = getProperty(IMPERSONATION_USERS);

		Map<String, String> map = Splitter.on(';').withKeyValueSeparator("=").split(allPermissions);
		String userPermissions = map.get(userName);

		if (userPermissions == null) {
			return null;
		}

		Iterable<String> perms = Splitter.on(',').split(userPermissions);
		for (String p : perms) {
			if (p.equals("*") || p.equals(proxyUser))
				return proxyUser;
		}

		return null;
	}

	protected String mapUserGroups(List<String> groupList) {
		if (groupList == null || groupList.isEmpty())
			return null;

		String mapGroupAttrStr = getProperty(LDAP_GROUP_ATTR);
		if (mapGroupAttrStr != null) {
			HashMap<String, String> groupmap = parseGroupAttr(mapGroupAttrStr);
			updateGroupList(groupList, groupmap);
		}
		return groupList.toString().replace("[", "")
				.replace("]", "").replace(", ",",");
	}

	/**
	 * Parse and prepare hashmap of group mappings
	 *
	 * @param mapGroupAttrStr ldap group to splice user mapping string as provided
	 * @return hashmap of ldap group to splice user map
	 */
	private HashMap<String,String> parseGroupAttr(String mapGroupAttrStr) {
		HashMap<String,String> groupAttrMap = new HashMap<>();
		String attrArr[] = mapGroupAttrStr.split(",");
		for (String elem : attrArr) {
			String mapAttr[] = elem.split("=");
			if (mapAttr.length == 2) {
				//add the mapping, but ignore the case for the cn from ldap
				groupAttrMap.put(mapAttr[0].trim().toLowerCase(), mapAttr[1].trim());
			}
		}
		return groupAttrMap;
	}


	/**
	 * Update the group list with the override property
	 * @param grouplist list of ldap groups tobe updated
	 * @param groupmap Map to override with
	 */
	private static void updateGroupList(List<String> grouplist, HashMap<String, String> groupmap) {
		if (groupmap.isEmpty())
			return;

		// Go through the grouplist and replace groupnames with the override name
		for (int i = 0; i < grouplist.size(); i++) {
			//ignore the case for the cn from ldap when looking up the mapped splice correspondent.
			String mappedGroupName = groupmap.get(grouplist.get(i).toLowerCase());
			if (mappedGroupName != null)
				grouplist.set(i, mappedGroupName);
		}
	}

	protected void setAuthenticationService(UserAuthenticator aScheme) {
		// specialized class is the principal caller.
		this.authenticationScheme = aScheme;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(this.authenticationScheme != null,
				"There is no authentication scheme for that service!");

			if (SanityManager.DEBUG_ON(AuthenticationTrace)) {

				java.io.PrintWriter iDbgStream =
					SanityManager.GET_DEBUG_STREAM();

				iDbgStream.println("Authentication Service: [" +
								this.toString() + "]");
				iDbgStream.println("Authentication Scheme : [" +
								this.authenticationScheme.toString() + "]");
			}
		}
	}

	/**
	/*
	** Methods of module control - To be overriden
	*/

	/**
		Start this module.  In this case, nothing needs to be done.
		@see com.splicemachine.db.iapi.services.monitor.ModuleControl#boot

		@exception StandardException upon failure to load/boot
		the expected authentication service.
	 */
	 public void boot(boolean create, Properties properties)
	  throws StandardException
	 {
			//
			// we expect the Access factory to be available since we're
			// at boot stage.
			//
			store = (AccessFactory)
				Monitor.getServiceModule(this, AccessFactory.MODULE);
			// register to be notified upon db properties changes
			// _only_ if we're on a database context of course :)

			PropertyFactory pf = (PropertyFactory)
				Monitor.getServiceModule(this, Module.PropertyFactory);
			if (pf != null)
				pf.addPropertySetNotification(this);

	 }

	/**
	 * @see com.splicemachine.db.iapi.services.monitor.ModuleControl#stop
	 */
	public void stop() {

		// nothing special to be done yet.
	}
	/*
	** Methods of AuthenticationService
	*/

	/**
	 * Authenticate a User inside JBMS.T his is an overload method.
	 *
	 * We're passed-in a Properties object containing user credentials information
	 * (as well as database name if user needs to be validated for a certain
	 * database access).
	 *
	 * @see
	 * com.splicemachine.db.iapi.jdbc.AuthenticationService#authenticate
	 *
	 *
	 */
	public String authenticate(String databaseName, Properties userInfo) throws java.sql.SQLException
	{
		if (userInfo == (Properties) null)
			return null;

		String userName = userInfo.getProperty(Attribute.USERNAME_ATTR);
		if ((userName != null) && userName.length() > Limits.MAX_IDENTIFIER_LENGTH) {
			return null;
		}

		if (SanityManager.DEBUG)
		{
			if (SanityManager.DEBUG_ON(AuthenticationTrace)) {

				java.io.PrintWriter iDbgStream =
					SanityManager.GET_DEBUG_STREAM();

//				iDbgStream.println(
//								" - Authentication request: user [" +
//							    userName + "]"+ ", database [" +
//							    databaseName + "]");
				// The following will print the stack trace of the
				// authentication request to the log.
				//Throwable t = new Throwable();
				//istream.println("Authentication Request Stack trace:");
				//t.printStackTrace(istream.getPrintWriter());
			}
		}
		// Authenticated username maybe a groupname for LDAP
		String authUser = this.authenticationScheme.authenticateUser(userName,
						  userInfo.getProperty(Attribute.PASSWORD_ATTR),
						  databaseName,
						  userInfo
						 );
		return authUser;
	}

    public  String  getSystemCredentialsDatabaseName()    { return null; }

	/**
	 * Returns a property if it was set at the database or
	 * system level. Treated as SERVICE property by default.
	 *
	 * @return a property string value.
	 **/
	public String getProperty(String key) {

		String propertyValue = null;
		TransactionController tc = null;

		try {

          tc = getTransaction();

		  propertyValue =
			PropertyUtil.getServiceProperty(tc,
											key,
											(String) null);
		  if (tc != null) {
			tc.commit();
			tc = null;
		  }

		} catch (StandardException se) {
			// Do nothing and just return
		}

		return propertyValue;
	}

    /**
     * <p>
     * Get a transaction for performing authentication at the database level.
     * </p>
     */
    protected   TransactionController   getTransaction()
        throws StandardException
    {
        if ( store == null ) { return null; }
        else
        {
            return store.getTransaction( ContextService.getFactory().getCurrentContextManager() );
        }
    }

    /**
     * Get all the database properties.
     * @return the database properties, or {@code null} if there is no
     * access factory
     */
    Properties getDatabaseProperties() throws StandardException {
        Properties props = null;

        TransactionController tc = getTransaction();
        if (tc != null) {
            try {
                props = tc.getProperties();
            } finally {
                tc.commit();
            }
        }

        return props;
    }

    /**
     * <p>
     * Get the name of the database if we are performing authentication at the database level.
     * </p>
     */
    protected   String  getServiceName()
    {
        if ( store == null ) { return null; }
        else { return Monitor.getServiceName( store ); }
    }

	public String getDatabaseProperty(String key) {

		String propertyValue = null;
		TransactionController tc = null;

		try {

		  if (store != null)
			tc = store.getTransaction(
                ContextService.getFactory().getCurrentContextManager());

		  propertyValue =
			PropertyUtil.getDatabaseProperty(tc, key);

		  if (tc != null) {
			tc.commit();
			tc = null;
		  }

		} catch (StandardException se) {
			// Do nothing and just return
		}

		return propertyValue;
	}

	public String getSystemProperty(String key) {

		boolean dbOnly = false;
		dbOnly = Boolean.valueOf(
				this.getDatabaseProperty(
						Property.DATABASE_PROPERTIES_ONLY));

		if (dbOnly)
			return null;

		return PropertyUtil.getSystemProperty(key);
	}

	/*
	** Methods of PropertySetCallback
	*/
	public void init(boolean dbOnly, Dictionary p) {
		// not called yet ...
	}

	/**
	  @see PropertySetCallback#validate
	*/
	public boolean validate(String key, Serializable value, Dictionary p)
        throws StandardException
    {

        // user password properties need to be remapped. nothing else needs remapping.
		if ( key.startsWith(Property.USER_PROPERTY_PREFIX) ) { return true; }

        String      stringValue = (String) value;
        boolean     settingToNativeLocal = Property.AUTHENTICATION_PROVIDER_NATIVE_LOCAL.equals( stringValue );
        
        if ( Property.AUTHENTICATION_PROVIDER_PARAMETER.equals( key ) )
        {
            // NATIVE + LOCAL is the only value of this property which can be persisted
            if (
                ( stringValue != null ) &&
                ( stringValue.startsWith( Property.AUTHENTICATION_PROVIDER_NATIVE ) )&&
                !settingToNativeLocal
                )
            {
                throw  StandardException.newException( SQLState.PROPERTY_DBO_LACKS_CREDENTIALS );
            }

            // once set to NATIVE authentication, you can't change it
            String  oldValue = (String) p.get( Property.AUTHENTICATION_PROVIDER_PARAMETER );
            if ( (oldValue != null) && oldValue.startsWith( Property.AUTHENTICATION_PROVIDER_NATIVE ) )
            {
                throw StandardException.newException( SQLState.PROPERTY_CANT_UNDO_NATIVE );
            }

            // can't turn on NATIVE + LOCAL authentication unless the DBO's credentials are already stored.
            // this should prevent setting NATIVE + LOCAL authentication in pre-10.9 databases too
            // because you can't store credentials in a pre-10.9 database.
            if ( settingToNativeLocal )
            {
                DataDictionary  dd = getDataDictionary();
                String              dbo = dd.getAuthorizationDatabaseOwner();
                UserDescriptor  userCredentials = dd.getUser( dbo );

                if ( userCredentials == null )
                {
                    throw StandardException.newException( SQLState.PROPERTY_DBO_LACKS_CREDENTIALS );
                }
            }
        }

        if ( Property.AUTHENTICATION_NATIVE_PASSWORD_LIFETIME.equals( key ) )
        {
            if ( parsePasswordLifetime( stringValue ) == null )
            {
                throw StandardException.newException
                    ( SQLState.BAD_PASSWORD_LIFETIME, Property.AUTHENTICATION_NATIVE_PASSWORD_LIFETIME );
            }
        }
        
        if ( Property.AUTHENTICATION_PASSWORD_EXPIRATION_THRESHOLD.equals( key ) )
        {
            if ( parsePasswordThreshold( stringValue ) == null )
            {
                throw StandardException.newException
                    ( SQLState.BAD_PASSWORD_LIFETIME, Property.AUTHENTICATION_PASSWORD_EXPIRATION_THRESHOLD );
            }
        }
        
        return false;
	}
    /** Parse the value of the password lifetime property. Return null if it is bad. */
    protected   Long    parsePasswordLifetime( String passwordLifetimeString )
    {
            try {
                long    passwordLifetime = Long.parseLong( passwordLifetimeString );

                if ( passwordLifetime < 0L ) { passwordLifetime = 0L; }

                return passwordLifetime;
            } catch (Exception e) { return null; }
    }
    /** Parse the value of the password expiration threshold property. Return null if it is bad. */
    protected   Double  parsePasswordThreshold( String expirationThresholdString )
    {
            try {
                double  expirationThreshold = Double.parseDouble( expirationThresholdString );

                if ( expirationThreshold <= 0L ) { return null; }
                else { return expirationThreshold; }
            } catch (Exception e) { return null; }
    }
    
	/**
	  @see PropertySetCallback#validate
	*/
	@Override
	public Serviceable apply(String key,Serializable value,Dictionary p, TransactionController tc)
	{
		return null;
	}
	/**
	  @see PropertySetCallback#map
	  @exception StandardException Thrown on error.
	*/
	public Serializable map(String key, Serializable value, Dictionary p)
		throws StandardException
	{
		// We only care for "derby.user." property changes
		// at the moment.
		if (!key.startsWith(Property.USER_PROPERTY_PREFIX)) return null;
		// We do not hash 'db.user.<userName>' password if
		// the configured authentication service is LDAP as the
		// same property could be used to store LDAP user full DN (X500).
		// In performing this check we only consider database properties
		// not system, service or application properties.

		String authService =
			(String)p.get(Property.AUTHENTICATION_PROVIDER_PARAMETER);

		if ((authService != null) &&
			 (StringUtil.SQLEqualsIgnoreCase(authService, Property.AUTHENTICATION_PROVIDER_LDAP)))
			return null;

		// Ok, we can hash this password in the db
		String userPassword = (String) value;

		if (userPassword != null) {
			// hash (digest) the password
			// the caller will retrieve the new value
            String userName =
                    key.substring(Property.USER_PROPERTY_PREFIX.length());
            userPassword =
                    hashUsingDefaultAlgorithm(userName, userPassword, p);
		}

		return userPassword;
	}


	// Class implementation

	protected final boolean requireAuthentication(Properties properties) {

		//
		// we check if db.connection.requireAuthentication system
		// property is set to true, otherwise we are the authentication
		// service that should be run.
		//
		String requireAuthentication = PropertyUtil.getPropertyFromSet(
					properties,
					Property.REQUIRE_AUTHENTICATION_PARAMETER
														);
		if (Boolean.valueOf(requireAuthentication)) { return true; }

        //
        // NATIVE authentication does not require that you set REQUIRE_AUTHENTICATION_PARAMETER.
        //
        return PropertyUtil.nativeAuthenticationEnabled( properties );
	}

	/**
     * <p>
	 * This method hashes a clear user password using a
	 * Single Hash algorithm such as SHA-1 (SHA equivalent)
	 * (it is a 160 bits digest)
     * </p>
	 *
     * <p>
	 * The digest is returned as an object string.
     * </p>
     *
     * <p>
     * This method is only used by the SHA-1 authentication scheme.
     * </p>
	 *
	 * @param plainTxtUserPassword Plain text user password
	 *
	 * @return hashed user password (digest) as a String object
     *         or {@code null} if the plaintext password is {@code null}
	 */
	protected String hashPasswordSHA1Scheme(String plainTxtUserPassword)
	{
		if (plainTxtUserPassword == null)
			return null;

		MessageDigest algorithm = null;
		try
		{
			algorithm = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException nsae)
		{
					// Ignore as we checked already during service boot-up
		}

        assert algorithm != null;
		algorithm.reset();
		byte[] bytePasswd = null;
        bytePasswd = toHexByte(plainTxtUserPassword);
		algorithm.update(bytePasswd);
		byte[] hashedVal = algorithm.digest();
		return (PasswordHasher.ID_PATTERN_SHA1_SCHEME +
                StringUtil.toHexString(hashedVal, 0, hashedVal.length));

	}

    /**
     * <p>
     * Convert a string into a byte array in hex format.
     * </p>
     *
     * <p>
     * For each character (b) two bytes are generated, the first byte
     * represents the high nibble (4 bits) in hexadecimal ({@code b & 0xf0}),
     * the second byte represents the low nibble ({@code b & 0x0f}).
     * </p>
     *
     * <p>
     * The character at {@code str.charAt(0)} is represented by the first two
     * bytes in the returned String.
     * </p>
     *
     * <p>
     * New code is encouraged to use {@code String.getBytes(String)} or similar
     * methods instead, since this method does not preserve all bits for
     * characters whose codepoint exceeds 8 bits. This method is preserved for
     * compatibility with the SHA-1 authentication scheme.
     * </p>
     *
     * @param str string
     * @return the byte[] (with hexadecimal format) form of the string (str)
     */
    private static byte[] toHexByte(String str)
    {
        byte[] data = new byte[str.length() * 2];

        for (int i = 0; i < str.length(); i++)
        {
            char ch = str.charAt(i);
            int high_nibble = (ch & 0xf0) >>> 4;
            int low_nibble = (ch & 0x0f);
            data[i] = (byte)high_nibble;
            data[i+1] = (byte)low_nibble;
        }
        return data;
    }

    /**
     * <p>
     * Hash a password using the default message digest algorithm for this
     * system before it's stored in the database.
     * </p>
     *
     * <p>
     * If the data dictionary supports the configurable hash authentication
     * scheme, and the property {@code db.authentication.builtin.algorithm}
     * is a non-empty string, the password will be hashed using the
     * algorithm specified by that property. Otherwise, we fall back to the new
     * authentication scheme based on SHA-1. The algorithm used is encoded in
     * the returned token so that the code that validates a user's credentials
     * knows which algorithm to use.
     * </p>
     *
     * @param user the user whose password to hash
     * @param password the plain text password
     * @param props database properties
     * @return a digest of the user name and password formatted as a string,
     *         or {@code null} if {@code password} is {@code null}
     * @throws StandardException if the specified algorithm is not supported
     */
    String hashUsingDefaultAlgorithm(String user,
                                                String password,
                                                Dictionary props)
            throws StandardException
    {
        if ( password ==  null ) { return null; }

        PasswordHasher  hasher = getDataDictionary().makePasswordHasher( props );

        if ( hasher != null ) { return hasher.hashAndEncode( user, password ); }
        else { return hashPasswordSHA1Scheme(password); }
    }

    /**
     * Find the data dictionary for the current connection.
     *
     * @return the {@code DataDictionary} for the current connection
     */
    private static DataDictionary getDataDictionary() {
        LanguageConnectionContext lcc = (LanguageConnectionContext)
            ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);
        return lcc.getDataDictionary();
    }

    /**
     * Strong Password Substitution (USRSSBPWD).
     *
     * This method generates a password substitute to authenticate a client
     * which is using a DRDA security mechanism such as SECMEC_USRSSBPWD.
     *
     * Depending how the user is defined in Derby and if BUILTIN
     * is used, the stored password can be in clear-text (system level)
     * or encrypted (hashed - *not decryptable*)) (database level) - If the
     * user has authenticated at the network level via SECMEC_USRSSBPWD, it
     * means we're presented with a password substitute and we need to
     * generate a substitute password coming from the store to compare with
     * the one passed-in.
     *
     * The substitution algorithm used is the same as the one used in the
     * SHA-1 authentication scheme ({@link PasswordHasher#ID_PATTERN_SHA1_SCHEME}), so in
     * the case of database passwords stored using that scheme, we can simply
     * compare the received hash with the stored hash. If the configurable
     * hash authentication scheme {@link PasswordHasher#ID_PATTERN_CONFIGURABLE_HASH_SCHEME}
     * is used, we have no way to find out if the received hash matches the
     * stored password, since we cannot decrypt the hashed passwords and
     * re-apply another hash algorithm. Therefore, strong password substitution
     * only works if the database-level passwords are stored with the SHA-1
     * scheme.
     *
     * NOTE: A lot of this logic could be shared with the DRDA decryption
     *       and client encryption managers - This will be done _once_
     *       code sharing along with its rules are defined between the
     *       Derby engine, client and network code (PENDING).
     * 
     * Substitution algorithm works as follow:
     *
     * PW_TOKEN = SHA-1(PW, ID)
     * The password (PW) and user name (ID) can be of any length greater
     * than or equal to 1 byte.
     * The client generates a 20-byte password substitute (PW_SUB) as follows:
     * PW_SUB = SHA-1(PW_TOKEN, RDr, RDs, ID, PWSEQs)
     * 
     * w/ (RDs) as the random client seed and (RDr) as the server one.
     * 
     * See PWDSSB - Strong Password Substitution Security Mechanism
     * (DRDA Vol.3 - P.650)
     *
	 * @return a substituted password.
     */
    protected String substitutePassword(
                String userName,
                String password,
                Properties info,
                boolean databaseUser) {

        MessageDigest messageDigest = null;

        // PWSEQs's 8-byte value constant - See DRDA Vol 3
        byte SECMEC_USRSSBPWD_PWDSEQS[] = {
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00,
                (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01
                };
        
        // Generated password substitute
        byte[] passwordSubstitute;

        try
        {
            messageDigest = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException nsae)
        {
            // Ignore as we checked already during service boot-up
        }
        // IMPORTANT NOTE: As the password is stored single-hashed in the
        // database, it is impossible for us to decrypt the password and
        // recompute a substitute to compare with one generated on the source
        // side - Hence, we have to generate a password substitute.
        // In other words, we cannot figure what the original password was -
        // Strong Password Substitution (USRSSBPWD) cannot be supported for
        // targets which can't access or decrypt passwords on their side.
        //
        assert messageDigest != null;
        messageDigest.reset();

        byte[] bytePasswd = null;
        byte[] userBytes = toHexByte(userName);

        if (SanityManager.DEBUG)
        {
            // We must have a source and target seed 
            SanityManager.ASSERT(
              (((String) info.getProperty(Attribute.DRDA_SECTKN_IN) != null) &&
              ((String) info.getProperty(Attribute.DRDA_SECTKN_OUT) != null)), 
                "Unexpected: Requester or server seed not available");
        }

        // Retrieve source (client)  and target 8-byte seeds
        String sourceSeedstr = info.getProperty(Attribute.DRDA_SECTKN_IN);
        String targetSeedstr = info.getProperty(Attribute.DRDA_SECTKN_OUT);

        byte[] sourceSeed_ =
            StringUtil.fromHexString(sourceSeedstr, 0, sourceSeedstr.length());
        byte[] targetSeed_ =
            StringUtil.fromHexString(targetSeedstr, 0, targetSeedstr.length());

        String hexString = null;
        // If user is at the database level, we don't hash the password
        // as it is already hashed (BUILTIN scheme) - we only do the
        // BUILTIN hashing if the user is defined at the system level
        // only - this is required beforehands so that we can do the password
        // substitute generation right afterwards.
        if (!databaseUser)
        {
            bytePasswd = toHexByte(password);
            messageDigest.update(bytePasswd);
            byte[] hashedVal = messageDigest.digest();
            hexString = PasswordHasher.ID_PATTERN_SHA1_SCHEME +
                StringUtil.toHexString(hashedVal, 0, hashedVal.length);
        }
        else
        {
            // Already hashed from the database store
            // NOTE: If the password was stored with the configurable hash
            // authentication scheme, the stored password will have been hashed
            // with a different algorithm than the hashed password sent from
            // the client. Since there's no way to decrypt the stored password
            // and rehash it with the algorithm that the client uses, we are
            // not able to compare the passwords, and the connection attempt
            // will fail.
            hexString = password;
        }

        // Generate the password substitute now

        // Generate some 20-byte password token
        messageDigest.update(userBytes);
        messageDigest.update(toHexByte(hexString));
        byte[] passwordToken = messageDigest.digest();
        
        // Now we generate the 20-byte password substitute
        messageDigest.update(passwordToken);
        messageDigest.update(targetSeed_);
        messageDigest.update(sourceSeed_);
        messageDigest.update(userBytes);
        messageDigest.update(SECMEC_USRSSBPWD_PWDSEQS);

        passwordSubstitute = messageDigest.digest();

        return StringUtil.toHexString(passwordSubstitute, 0,
                                      passwordSubstitute.length);
    }


	/**
	 * Create the DBO if it does not already exist in the local credentials database.
	 * @param userName		The DBO user's name used to connect to JBMS system
	 * @param userPassword	The DBO user's password used to connect to JBMS system
	 * @param dd			data dictionary to store the user
	 * @param tc			transaction for this operation
	 * @throws StandardException
	 * @throws SQLException
	 */
	protected void createDBOUserIfDoesNotExist(String userName, String userPassword, DataDictionary dd, TransactionController tc)
			throws StandardException, SQLException {
		// Check if the DBO already exists which may happen if the manual override for
		// creation of the native credentials database is set.
		if (dd.getUser(userName) == null) {
			SystemProcedures.addUser( userName, userPassword, tc );
			// Change the system schemas to be owned by the user.  This is needed for upgrading
			// the Splice Machine 0.5 beta where the owner of the system schemas was APP.
			// Splice Machine 1.0+ has the SPLICE user as the DBO of the system schemas.
			SystemProcedures.updateSystemSchemaAuthorization(userName, tc);
		}
	}

	/**
	 * Create the default schema for the DBO if it does not already exist in the local credentials database.
	 * @param userName		The DBO user's name used to connect to JBMS system
	 * @param dd			data dictionary to store the DBO schema
	 * @param tc			transaction for this operation
	 * @throws StandardException
	 * @throws SQLException
	 */
	protected void createDBOSchemaIfDoesNotExist(String userName, String userPassword, DataDictionary dd, TransactionController tc)
			throws StandardException, SQLException {
		LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

		// Check if the DBO schema already exists which may happen if the manual override for
		// creation of the native credentials database is set.
		SchemaDescriptor sd = dd.getSchemaDescriptor(userName, tc, false);
		if (sd == null || sd.getUUID() == null) {
			UUID tmpSchemaId = dd.getUUIDFactory().createUUID();
			dd.startWriting(lcc);
			sd = ddg.newSchemaDescriptor(userName, userName, tmpSchemaId);
			dd.addDescriptor(sd, null, DataDictionary.SYSSCHEMAS_CATALOG_NUM, false, tc, false);
		}
	}
}
