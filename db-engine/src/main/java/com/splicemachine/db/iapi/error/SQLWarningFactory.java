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

package com.splicemachine.db.iapi.error;

import java.sql.SQLWarning;

import com.splicemachine.db.iapi.services.i18n.MessageService;


// for javadoc 
import com.splicemachine.db.shared.common.reference.SQLState;

/**
 * This class generates SQLWarning instances. It has an understanding of Derby's
 * internal error/warning message Ids, and transforms these to localised error
 * messages and appropriate SQLState.
 */
public class SQLWarningFactory {

	/**
	 * Generates a SQLWarning instance based on the supplied messageId. It looks
	 * up the messageId to generate a localised warning message. Also, SQLState
	 * is set correctly based on the messageId.
	 * 
	 * @param messageId A Derby messageId as defined in{@link SQLState com.splicemachine.db.shared.common.reference.SQLState}.
	 * @return Properly initialized SQLWarning instance.
	 * @see com.splicemachine.db.shared.common.reference.SQLState
	 */
	public static SQLWarning newSQLWarning( String messageId )
    {
		return newSQLWarning(messageId, new Object[] {} );
	}

	/**
	 * Generates a SQLWarning instance based on the supplied messageId and
	 * argument. It looks up the messageId to generate a localised warning
	 * message. Also, SQLState is set correctly based on the messageId.
	 * 
	 * @param messageId A Derby messageId as defined in {@link SQLState com.splicemachine.db.shared.common.reference.SQLState}.
	 * @param arg1 An argument for the warning message
	 * @return Properly initialized SQLWarning instance.
	 * @see com.splicemachine.db.shared.common.reference.SQLState
	 */
	public static SQLWarning newSQLWarning( String messageId, Object arg1 )
    {
        return newSQLWarning( messageId, new Object[] { arg1 } );
	}

	/**
	 * Generates a SQLWarning instance based on the supplied messageId and
	 * arguments. It looks up the messageId to generate a localised warning
	 * message. Also, SQLState is set correctly based on the messageId.
	 * 
	 * @param messageId
	 *            A Derby messageId as defined in {@link SQLState com.splicemachine.db.shared.common.reference.SQLState}.
	 * @param arg1 First argument for the warning message
	 * @param arg2 Second argument for the warning message
	 * @return Properly initialized SQLWarning instance.
	 * @see com.splicemachine.db.shared.common.reference.SQLState
	 */
	public static SQLWarning newSQLWarning( String messageId, Object arg1, Object arg2 )
    {
        return newSQLWarning( messageId, new Object[] { arg1, arg2 } );
	}

	/**
	 * Generates a SQLWarning instance based on the supplied messageId and
	 * arguments. It looks up the messageId to generate a localised warning
	 * message. Also, SQLState is set correctly based on the messageId.
	 * 
	 * @param messageId A Derby messageId as defined in {@link SQLState com.splicemachine.db.shared.common.reference.SQLState}.
	 * @param args Arguments for the warning message
	 * @return Properly initialized SQLWarning instance.
	 * @see com.splicemachine.db.shared.common.reference.SQLState
	 */
	public static SQLWarning newSQLWarning( String messageId, Object[] args )
    {
		return new SQLWarning
            (
             MessageService.getCompleteMessage( messageId, args ),
             StandardException.getSQLStateFromIdentifier(messageId),
             ExceptionSeverity.WARNING_SEVERITY
             );
	}

}
