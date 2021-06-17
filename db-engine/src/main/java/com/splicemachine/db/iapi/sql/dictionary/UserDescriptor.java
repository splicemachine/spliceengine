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
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;

import java.sql.Timestamp;
import java.util.Arrays;

/**
 * A Descriptor for a user stored in SYSUSERS.
 */
public final class  UserDescriptor extends TupleDescriptor 
{
    private String _userName;
    private String _hashingScheme;
    private char[] _password;
    private Timestamp _lastModified;
    private UUID _databaseId;

    /**
     * Constructor for a UserDescriptor.
     *  @param dataDictionary        The data dictionary that this descriptor lives in.
     * @param userName  Name of the user.
     * @param hashingScheme How the password was hashed.
     * @param password  The user's password.
     * @param lastModified  Time that the password was last modified.
     * @param databaseId
     */

    public UserDescriptor
    (
            DataDictionary dataDictionary,
            String userName,
            String hashingScheme,
            char[] password,
            Timestamp lastModified,
            UUID databaseId)
    {
        super( dataDictionary );

        _userName = userName;
        _hashingScheme = hashingScheme;

        if ( password == null ) { _password = null; }
        else
        {
            // copy the password because the caller will 0 it out
            _password = new char[ password.length ];
            System.arraycopy( password, 0, _password, 0, password.length );
        }
        
        _lastModified = lastModified;
        _databaseId = databaseId;
    }

    public String getUserName(){ return _userName; }
    public String getHashingScheme()    { return _hashingScheme; }
    public  Timestamp   getLastModified()   { return _lastModified; }

    /**
     * <p>
     * Zero the password after getting it so that the char[] can't be memory-sniffed.
     * </p>
     */
    public char[]   getAndZeroPassword()
    {
        if (_password == null)
                return null;
        int length = _password.length;
        char[] retval = new char[ length ];
        System.arraycopy( _password, 0, retval, 0, length );
        Arrays.fill( _password, (char) 0 );

        return retval;
    }

    //
    // class interface
    //


    /** @see TupleDescriptor#getDescriptorType */
    public String getDescriptorType() { return "User"; }

    /** @see TupleDescriptor#getDescriptorName */
    public String getDescriptorName() { return _userName; }

    public UUID getDatabaseId() {
        return _databaseId;
    }

    public void setDatabaseId(UUID dbId) {
        _databaseId = dbId;
    }

    private boolean isDbOwner() {
        String dbo = dataDictionary.getAuthorizationDatabaseOwner(_databaseId);

        return dbo.equals(this._userName);
    }

    UserDescriptor cloneForOtherDatabase(UUID otherDbUUID) {
        return new UserDescriptor(dataDictionary, _userName, _hashingScheme, getAndZeroPassword(), _lastModified, otherDbUUID);
    }

    public void drop(LanguageConnectionContext lcc, boolean dropIfOwner) throws StandardException {
        DataDictionary dd=lcc.getDataDictionary();

        // you can't drop the credentials of the dbo
        if (!dropIfOwner && isDbOwner()) {
            throw StandardException.newException(SQLState.CANT_DROP_DBO);
        }

        /*
         ** Inform the data dictionary that we are about to write to it.
         ** There are several calls to data dictionary "get" methods here
         ** that might be done in "read" mode in the data dictionary, but
         ** it seemed safer to do this whole operation in "write" mode.
         **
         ** We tell the data dictionary we're done writing at the end of
         ** the transaction.
         */
        dd.startWriting(lcc);

        dd.dropUser(_databaseId, _userName, lcc.getTransactionExecute());
    }
}
