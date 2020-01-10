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

package com.splicemachine.db.impl.tools.dblook;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.splicemachine.db.tools.dblook;

/**
 * Dblook implementation for SEQUENCEs.
 */
public class DB_Sequence
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////


    ///////////////////////////////////////////////////////////////////////////////////
    //
    // BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////


	/**
     * <p>
	 * Generate the DDL for all sequences and output it via Logs.java.
     * </p>
     *
	 * @param conn Connection to the source database.
     */

	public static void doSequences( Connection conn )
		throws SQLException
    {
		PreparedStatement ps = conn.prepareStatement
            (
             "SELECT SCHEMAID, SEQUENCENAME, SEQUENCEDATATYPE, STARTVALUE, MINIMUMVALUE, MAXIMUMVALUE, INCREMENT, CYCLEOPTION\n" +
             "FROM SYS.SYSSEQUENCES"
             );
        ResultSet rs = ps.executeQuery();

		boolean firstTime = true;
		while (rs.next())
        {
            int  col = 1;
            String schemaName = dblook.lookupSchemaId( rs.getString( col++ ) );
            String sequenceName = rs.getString( col++ );
            String typeName = stripNotNull( rs.getString( col++ ) );
            long startValue = rs.getLong( col++ );
            long minimumValue = rs.getLong( col++ );
            long maximumValue = rs.getLong( col++ );
            long increment = rs.getLong( col++ );
            String cycleOption = "Y".equals( rs.getString( col++ ) ) ? "CYCLE" : "NO CYCLE";

			if (firstTime)
            {
				Logs.reportString("----------------------------------------------");
                Logs.reportMessage( "DBLOOK_SequenceHeader" );
				Logs.reportString("----------------------------------------------\n");
			}

			String fullName = dblook.addQuotes( dblook.expandDoubleQuotes( sequenceName ) );
			fullName = schemaName + "." + fullName;

			String creationString = createSequenceString
                ( fullName, typeName, startValue, minimumValue, maximumValue, increment, cycleOption );
			Logs.writeToNewDDL(creationString);
			Logs.writeStmtEndToNewDDL();
			Logs.writeNewlineToNewDDL();
			firstTime = false;
		}

        rs.close();
        ps.close();
	}
    /** Strip the trailing NOT NULL off of the string representation of a datatype */
    private static String stripNotNull( String datatypeName )
    {
        int idx = datatypeName.indexOf( "NOT" );
        if ( idx > 0 ) { return datatypeName.substring( 0, idx ); }
        else { return datatypeName; }
    }

	/**
     * <p>
	 * Generate DDL for a specific sequence.
     * </p>
     *
     * @param fullName Fully qualified name of the sequence
     * @param dataTypeName Name of the datatype of the sequence
     * @param startValue First value to use in the range of the sequence
     * @param minimumValue Smallest value in the range
     * @param maximumValue Largest value in the range
     * @param increment Step size of the sequence
     * @param cycleOption CYCLE or NO CYCLE
     *
	 * @return DDL for the current stored sequence
     */
	private static String createSequenceString
        (
         String fullName,
         String dataTypeName,
         long startValue,
         long minimumValue,
         long maximumValue,
         long increment,
         String cycleOption
         )
		throws SQLException
	{
        String buffer = "CREATE SEQUENCE " + fullName + '\n' +
                "    AS " + dataTypeName + '\n' +
                "    START WITH " + Long.toString(startValue) + '\n' +
                "    INCREMENT BY " + Long.toString(increment) + '\n' +
                "    MAXVALUE " + Long.toString(maximumValue) + '\n' +
                "    MINVALUE " + Long.toString(minimumValue) + '\n' +
                "    " + cycleOption + '\n';

        return buffer;
	}

}
