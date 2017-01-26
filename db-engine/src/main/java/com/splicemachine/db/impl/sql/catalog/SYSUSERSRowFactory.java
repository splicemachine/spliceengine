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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.catalog;

import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.sql.dictionary.CatalogRowFactory;
import com.splicemachine.db.iapi.sql.dictionary.DataDescriptorGenerator;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.UserDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SystemColumn;
import com.splicemachine.db.iapi.sql.dictionary.TupleDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.types.SQLTimestamp;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 * Factory for creating a SYSUSERS row.
 */

public class SYSUSERSRowFactory extends CatalogRowFactory
{
	public static final String	TABLE_NAME = "SYSUSERS";
    public  static  final   String  SYSUSERS_UUID = "9810800c-0134-14a5-40c1-000004f61f90";
    public  static  final   String  PASSWORD_COL_NAME = "PASSWORD";
    
    private static final int		SYSUSERS_COLUMN_COUNT = 4;

	/* Column #s (1 based) */
    public static final int		USERNAME_COL_NUM = 1;
    public static final int		HASHINGSCHEME_COL_NUM = 2;
    public static final int		PASSWORD_COL_NUM = 3;
    public static final int		LASTMODIFIED_COL_NUM = 4;

    static final int		SYSUSERS_INDEX1_ID = 0;

	private static final int[][] indexColumnPositions =
	{
		{USERNAME_COL_NUM},
	};

    private	static	final	boolean[]	uniqueness = null;

	private	static	final	String[]	uuids =
	{
		SYSUSERS_UUID,	// catalog UUID
		"9810800c-0134-14a5-a609-000004f61f90",	// heap UUID
		"9810800c-0134-14a5-f1cd-000004f61f90",	// SYSUSERS_INDEX1
	};

	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

    public SYSUSERSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
	{
		super( uuidf, ef, dvf );
		initInfo( SYSUSERS_COLUMN_COUNT, TABLE_NAME, indexColumnPositions, uniqueness, uuids );
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

	/**
	 * Make a SYSUSERS row. The password in the UserDescriptor will be zeroed by
     * this method.
	 *
	 * @return	Row suitable for inserting into SYSUSERS
	 *
	 * @exception   StandardException thrown on failure
	 */

	public ExecRow makeRow( TupleDescriptor td, TupleDescriptor parent )
        throws StandardException
	{
		String  userName = null;
		String  hashingScheme = null;
		char[]  password = null;
		Timestamp   lastModified = null;
		
		ExecRow        			row;

        try {
            if ( td != null )	
            {
                UserDescriptor descriptor = (UserDescriptor) td;
                userName = descriptor.getUserName();
                hashingScheme = descriptor.getHashingScheme();
                password = descriptor.getAndZeroPassword();
                lastModified = descriptor.getLastModified();
            }
	
            /* Build the row to insert  */
            row = getExecutionFactory().getValueRow( SYSUSERS_COLUMN_COUNT );

            /* 1st column is USERNAME (varchar(128)) */
            row.setColumn( USERNAME_COL_NUM, new SQLVarchar( userName ) );

            /* 2nd column is HASHINGSCHEME (varchar(32672)) */
            row.setColumn( HASHINGSCHEME_COL_NUM, new SQLVarchar( hashingScheme ) );

            /* 3rd column is PASSWORD (varchar(32672)) */
            row.setColumn( PASSWORD_COL_NUM, new SQLVarchar( password ) );

            /* 4th column is LASTMODIFIED (timestamp) */
            row.setColumn( LASTMODIFIED_COL_NUM, new SQLTimestamp( lastModified ) );
        }
        finally
        {
            // zero out the password to prevent it from being memory-sniffed
            if ( password != null ) { Arrays.fill( password, (char) 0 ); }
        }

		return row;
	}

	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Make a descriptor out of a SYSUSERS row. The password column in the
     * row will be zeroed out.
	 *
	 * @param row a row
	 * @param parentTupleDescriptor	Null for this kind of descriptor.
	 * @param dd dataDictionary
	 *
	 * @return	a descriptor equivalent to a row
	 *
	 * @exception   StandardException thrown on failure
	 */
	public TupleDescriptor buildDescriptor(
		ExecRow					row,
		TupleDescriptor			parentTupleDescriptor,
		DataDictionary 			dd )
					throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			if (row.nColumns() != SYSUSERS_COLUMN_COUNT)
			{
				SanityManager.THROWASSERT("Wrong number of columns for a SYSUSERS row: "+
							 row.nColumns());
			}
		}

		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

		String	userName;
		String	hashingScheme;
		char[]  password = null;
		Timestamp   lastModified;
		DataValueDescriptor	col;
		SQLVarchar	passwordCol = null;

		UserDescriptor	result;

        try {
            /* 1st column is USERNAME */
            col = row.getColumn( USERNAME_COL_NUM );
            userName = col.getString();

            /* 2nd column is HASHINGSCHEME */
            col = row.getColumn( HASHINGSCHEME_COL_NUM );
            hashingScheme = col.getString();
		
            /* 3nd column is PASSWORD */
            passwordCol = (SQLVarchar) row.getColumn( PASSWORD_COL_NUM );
            password = passwordCol.getRawDataAndZeroIt();

            /* 4th column is LASTMODIFIED */
            col = row.getColumn( LASTMODIFIED_COL_NUM );
            lastModified = col.getTimestamp( new java.util.GregorianCalendar() );

            result = ddg.newUserDescriptor( userName, hashingScheme, password, lastModified );
        }
        finally
        {
            // zero out the password so that it can't be memory-sniffed
            if ( password != null ) { Arrays.fill( password, (char) 0 ); }
            if ( passwordCol != null ) { passwordCol.zeroRawData(); }
        }
        
		return result;
	}

	/**
	 * Builds a list of columns suitable for creating this Catalog.
	 *
	 *
	 * @return array of SystemColumn suitable for making this catalog.
	 */
    public SystemColumn[]   buildColumnList()
        throws StandardException
    {
        return new SystemColumn[]
        {
            SystemColumnImpl.getIdentifierColumn( "USERNAME", false ),
            SystemColumnImpl.getColumn( "HASHINGSCHEME", Types.VARCHAR, false, TypeId.VARCHAR_MAXWIDTH ),
            SystemColumnImpl.getColumn( PASSWORD_COL_NAME, Types.VARCHAR, false, TypeId.VARCHAR_MAXWIDTH ),
            SystemColumnImpl.getColumn( "LASTMODIFIED", Types.TIMESTAMP, false ),
        };
    }
}
