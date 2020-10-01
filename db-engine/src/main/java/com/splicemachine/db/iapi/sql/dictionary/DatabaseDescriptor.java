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

package com.splicemachine.db.iapi.sql.dictionary;

import com.splicemachine.db.catalog.Dependable;
import com.splicemachine.db.catalog.DependableFinder;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.depend.DependencyManager;
import com.splicemachine.db.iapi.sql.depend.Provider;
import com.splicemachine.db.iapi.store.access.TransactionController;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * This class represents a database descriptor
 *
 * @version 0.1
 */

public final class DatabaseDescriptor extends TupleDescriptor implements UniqueTupleDescriptor, Provider, Externalizable {

    /*
    ** When we boot, we create the DB spliceDB which will contain SYS,SYSIBM and SYSIBMADM schemas
    */
    /**
     * STD_DB_NAME is the name of the default database
     *
     */
    public static final String STD_DB_NAME = "SPLICEDB";

    /** the public interface for this system:
        <ol>
        <li>public String getDatabaseName();
        <li>public String getAuthorizationId();
        <li>public void    setUUID(UUID uuid);
        </ol>
    */

    //// Implementation
    private String name;
    private UUID oid;
    private String aid;

    /**
     * Needed for serialization...
     */
    public DatabaseDescriptor() {

    }
    /**
     * Constructor for a DatabaseDescriptor.
     *
     * @param dataDictionary
     * @param name          The database descriptor for this table.
     * @param aid           The authorization id
     * @param oid           The object id
     */
    public DatabaseDescriptor(DataDictionary  dataDictionary, String name, String aid, UUID oid)
    {
        super (dataDictionary);

        this.name = name;
        this.aid = aid;
        this.oid = oid;
    }

    /**
     * Gets the name of the database
     *
     * @return    The database name
     */
    public String    getDatabaseName()
    {
        return name;
    }

    /**
     * Gets the authorization id of the database
     *
     * @return    Authorization id
     *        lives in.
     */
    public String getAuthorizationId()
    {
        return aid;
    }

    /**
     * Sets the authorization id of the database. This is only used by the DataDictionary
     * during boot in order to patch up the authorization ids on system database.
     *
     * @param newAuthorizationID What is is
     */
    public void setAuthorizationId( String newAuthorizationID )
    {
        aid = newAuthorizationID;
    }

    /**
     * Gets the oid of the database
     *
     * @return    An oid
     */
    public UUID    getUUID()
    {
        return oid;
    }

    /**
     * Sets the oid of the database
     *
     * @param oid    The object id
     *
     */
    public void    setUUID(UUID oid)
    {
        this.oid = oid;
    }

    //
    // Provider interface
    //

    /**
        @return the stored form of this provider

            @see Dependable#getDependableFinder
     */
    public DependableFinder getDependableFinder()
    {
        throw new NotImplementedException();
    }

    /**
     * Return the name of this Provider.  (Useful for errors.)
     *
     * @return String    The name of this provider.
     */
    public String getObjectName()
    {
        return name;
    }

    /**
     * Get the provider's UUID
     *
     * @return String    The provider's UUID
     */
    public UUID getObjectID()
    {
        return oid;
    }

    /**
     * Get the provider's type.
     *
     * @return String        The provider's type.
     */
    public String getClassType()
    {
        return Dependable.DATABASE;
    }

    //
    // class interface
    //

    /**
     * Prints the contents of the DatabaseDescriptor
     *
     * @return The contents as a String
     */
    public String toString()
    {
        return name;
    }

    //    Methods so that we can put DatabaseDescriptors on hashed lists

    /**
      *    Determine if two DatabaseDescriptors are the same.
      *
      *    @param    otherObject    other DatabaseDescriptor
      *
      *    @return    true if they are the same, false otherwise
      */

    public boolean equals(Object otherObject)
    {
        if (!(otherObject instanceof DatabaseDescriptor))
            return false;

        DatabaseDescriptor other = (DatabaseDescriptor) otherObject;

        if ((oid != null) && (other.oid != null))
            return oid.equals( other.oid);

        return name.equals(other.name);
    }

    /**
      *    Get a hashcode for this DatabaseDescriptor
      *
      *    @return    hashcode
      */
    public int hashCode()
    {
        return oid.hashCode();
    }

    /** @see TupleDescriptor#getDescriptorName */
    public String getDescriptorName()
    {
        return name;
    }

    /** @see TupleDescriptor#getDescriptorType */
    public String getDescriptorType()
    {
        return "Database";
    }
    
    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException {
        name = input.readUTF();
        aid = input.readUTF();
        oid = (UUID) input.readObject();
    }

    public void writeExternal(ObjectOutput output) throws IOException {
        output.writeUTF(name);
        output.writeUTF(aid);
        output.writeObject(oid);
    }
    public void setDataDictionary(DataDictionary dataDictionary) {
        this.dataDictionary = dataDictionary;
    }

    /**
     * Drop this database.
     * Drops the database if it is empty.
     * @throws StandardException Schema could not be dropped.
     */
    public void drop(LanguageConnectionContext lcc,
                     Activation activation) throws StandardException
    {
        DataDictionary dd = getDataDictionary();
        DependencyManager dm = dd.getDependencyManager();
        TransactionController tc = lcc.getTransactionExecute();

        // First drop restrict the default SPLICE schema:
        SchemaDescriptor spliceSchemaDesc = dd.getSchemaDescriptor(getUUID(), Property.DEFAULT_USER_NAME, tc, false);
        if (spliceSchemaDesc != null) {
            spliceSchemaDesc.drop(lcc, activation);
        }

        /*
         ** Make sure the database is empty.
         ** In the future we want to drop everything
         ** in the database if it is CASCADE.
         */
        if (!dd.isDatabaseEmpty(this))
        {
            throw StandardException.newException(SQLState.LANG_DATABASE_NOT_EMPTY, getDatabaseName());
        }

        /* Prepare all dependents to invalidate.  (This is there chance
         * to say that they can't be invalidated.  For example, an open
         * cursor referencing a table/view that the user is attempting to
         * drop.) If no one objects, then invalidate any dependent objects.
         */
        dm.invalidateFor(this, DependencyManager.DROP_DATABASE, lcc);

        dd.dropDatabaseDescriptor(getDatabaseName(), tc);

        /*
         ** If we have dropped the current default databae,
         ** then we will set the default to null.  The
         ** LCC is free to set the new default database to
         ** some system defined default.
         */
        // lcc.resetSchemaUsages(activation, getDatabaseName()); XXX (arnaud multidb) implement that?
    }
}
