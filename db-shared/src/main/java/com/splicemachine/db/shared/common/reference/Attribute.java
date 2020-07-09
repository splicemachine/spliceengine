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

package com.splicemachine.db.shared.common.reference;

/**
 * List of all connection (JDBC) attributes by the system.
 * 
 * 
 * <P>
 * This class exists for two reasons
 * <Ol>
 * <LI> To act as the internal documentation for the attributes.
 * <LI> To remove the need to declare a java static field for the attributes
 * name in the protocol/implementation class. This reduces the footprint as the
 * string is final and thus can be included simply as a String constant pool
 * entry.
 * </OL>
 * <P>
 * This class should not be shipped with the product.
 * 
 * <P>
 * This class has no methods, all it contains are String's which by are public,
 * static and final since they are declared in an interface.
 */

public interface Attribute {

    /**
     * Not an attribute but the root for the JDBC URL that Derby supports.
     */
    String PROTOCOL = "jdbc:splice:";

    /**
     * The SQLJ protocol for getting the default connection for server side jdbc
     */
    String SQLJ_NESTED = "jdbc:default:connection";

    // Network Protocols. These need to be rejected by the embedded driver.

    /**
     * The protocol for Derby Network Client
     */
    String DNC_PROTOCOL = "jdbc:splice://";

    /**
     * The protocol for the IBM Universal JDBC Driver
     * 
     */
    String JCC_PROTOCOL = "jdbc:splice:net:";

    /**
     * User should use this prefix for the client attributes traceLevel 
     * and traceDirectory when they are sending those attributes as JVM 
     * properties. 
     * These 2 attributes can be sent through jdbc url directly (supported
     * way) or as JVM properties with the following prefix (undocumented 
     * way). DERBY-1275
     */
    String CLIENT_JVM_PROPERTY_PREFIX = "derby.client.";

    /**
     * Attribute name to encrypt the database on disk. If set to true, all user
     * data is stored encrypted on disk.
     */
    String DATA_ENCRYPTION = "dataEncryption";

    /**
     * If dataEncryption is true, use this attribute to pass in the secret key.
     * The secret key must be at least 8 characters long. This key must not be
     * stored persistently in cleartext anywhere.
     */

    String BOOT_PASSWORD = "bootPassword";

    /**
     * Attribute name to start replication primary mode for a database.
     * If used, REPLICATION_REPLICA_HOST is a required attribute.
     */
    String REPLICATION_START_PRIMARY = "startPrimary";

    /**
     * Attribute name to stop replication primary mode for a database.
     */
    String REPLICATION_STOP_PRIMARY = "stopPrimary";

    /**
     * Attribute name to start replication replica mode for a database.
     */
    String REPLICATION_START_REPLICA = "startReplica";

    /**
     * Attribute name to stop replication replica mode for a database.
     */
    String REPLICATION_STOP_REPLICA = "stopReplica";

    /**
     * Attribute name to stop replication replica mode for a database.
     * Internal use only
     */
    String REPLICATION_INTERNAL_SHUTDOWN_REPLICA = "internal_stopReplica";

    /**
     * If startPrimary is true, this attribute is used to specify the
     * host name the primary should connect to. This is a required
     * attribute.
     */
    String REPLICATION_REPLICA_HOST = "replicaHost";

    /**
     * Attribute name to start failover for a given database..
     */
    String REPLICATION_FAILOVER = "failover";

    /**
     * If startPrimary is true, this attribute is used to specify the
     * port the primary should connect to. This is an optional
     * attribute.
     */
    String REPLICATION_REPLICA_PORT = "replicaPort";

    /**
     * The attribute that is used for the database name, from the JDBC notion of
     * jdbc:<subprotocol>:<subname>
     */
    String DBNAME_ATTR = "databaseName";

    /**
     * The attribute that is used to request a shutdown.
     */
    String SHUTDOWN_ATTR = "shutdown";

    /**
     * The attribute that is used to request a database create.
     */
    String CREATE_ATTR = "create";

    /**
     * The attribute that is used to set the user name.
     */
    String USERNAME_ATTR = "user";

    /**
     * The attribute that is used to set the user password.
     */
    String PASSWORD_ATTR = "password";

    /**
     * The attribute that is used to set the connection's DRDA ID.
     */
    String DRDAID_ATTR = "drdaID";

    /**
     * The attribute that is used to allow upgrade.
     */
    String UPGRADE_ATTR = "upgrade";

    /**
     * Put the log on a different device.
     */
    String LOG_DEVICE = "logDevice";

    /**
     * Set the territory for the database.
     */
    String TERRITORY = "territory";

    /**
     * Set the collation sequence of the database, currently on IDENTITY will be
     * supported (strings will sort according to binary comparison).
     */
    String COLLATE = "collate";

    /**
     * Attribute for encrypting a database. Specifies the cryptographic services
     * provider.
     */
    String CRYPTO_PROVIDER = "encryptionProvider";

    /**
     * Attribute for encrypting a database. Specifies the cryptographic
     * algorithm.
     */
    String CRYPTO_ALGORITHM = "encryptionAlgorithm";

    /**
     * Attribute for encrypting a database. Specifies the key length in bytes
     * for the specified cryptographic algorithm.
     */
    String CRYPTO_KEY_LENGTH = "encryptionKeyLength";

    /**
     * Attribute for encrypting a database. Specifies the actual key. When this
     * is specified all the supplied crypto information is stored external to
     * the database, ie by the application.
     */
    String CRYPTO_EXTERNAL_KEY = "encryptionKey";

    /**
     * This attribute is used to request to create a database from backup. This
     * will throw error if a database with same already exists at the location
     * where we tring to create.
     */
    String CREATE_FROM = "createFrom";

    /**
     * This attribute is used to request a database restore from backup. It must
     * be used only when the active database is corrupted, because it will
     * cleanup the existing database and replace it from the backup.
     */
    String RESTORE_FROM = "restoreFrom";

    /**
     * The attribute that is used to request a roll-forward recovery of the
     * database.
     */
    String ROLL_FORWARD_RECOVERY_FROM = "rollForwardRecoveryFrom";

    /**
     * securityMechanism sets the mechanism for transmitting the user name and
     * password from the client. Client driver attribute.
     */
    String CLIENT_SECURITY_MECHANISM = "securityMechanism";

    /**
     * Kerberos Principal, implies security mechanism = KERBEROS
     */
    String CLIENT_KERBEROS_PRINCIPAL = "principal";

    /**
     * Kerberos keytab, requires setting CLIENT_KERBEROS_PRINCIPAL too
     */
    String CLIENT_KERBEROS_KEYTAB = "keytab";

    /**
     * traceFile sets the client side trace file. Client driver attribute.
     */
    String CLIENT_TRACE_FILE = "traceFile";

    /**
     * traceDirectory sets the client side trace directory.
     * Client driver attribute.
     */
    String CLIENT_TRACE_DIRECTORY = "traceDirectory";
    
    /**
     * traceFileAppend.
     * Client driver attribute.
     */
    String CLIENT_TRACE_APPEND = "traceFileAppend";
    
    /**
     * traceLevel.
     * Client driver attribute.
     */
    String CLIENT_TRACE_LEVEL = "traceLevel";
    
    /**
     * retrieveMessageText.
     * Client driver attribute.
     */    
    String CLIENT_RETIEVE_MESSAGE_TEXT = "retrieveMessageText";

    /**
       The attribute that is used to set client SSL mode.
    */
    String SSL_ATTR = "ssl";

}


