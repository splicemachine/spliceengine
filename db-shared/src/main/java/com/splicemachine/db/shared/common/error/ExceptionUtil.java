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
package com.splicemachine.db.shared.common.error;

/**
 * This class provides utility routines for exceptions 
 */
public class ExceptionUtil
{
   	/**
	 *  Convert a message identifer from 
     *  com.splicemachine.db.shared.common.reference.SQLState to
	 *  a SQLState five character string.
     *
	 *	@param messageID - the sql state id of the message from Derby
	 *	@return String 	 - the 5 character code of the SQLState ID to returned to the user 
	*/
	public static String getSQLStateFromIdentifier(String messageID) {

		if (messageID.length() == 5)
			return messageID;
		return messageID.substring(0, 5);
	}
    
   	/**
	* Get the severity given a message identifier from SQLState.
	*/
	public static int getSeverityFromIdentifier(String messageID) {

		int lseverity = ExceptionSeverity.NO_APPLICABLE_SEVERITY;

		switch (messageID.length()) {
		case 5:
			switch (messageID.charAt(0)) {
			case '0':
				switch (messageID.charAt(1)) {
				case '1':
					lseverity = ExceptionSeverity.WARNING_SEVERITY;
					break;
				case 'A':
				case '7':
					lseverity = ExceptionSeverity.STATEMENT_SEVERITY;
					break;
				case '8':
					lseverity = ExceptionSeverity.SESSION_SEVERITY;
					break;
				}
				break;	
			case '2':
			case '3':
				lseverity = ExceptionSeverity.STATEMENT_SEVERITY;
				break;
			case '4':
				switch (messageID.charAt(1)) {
				case '0':
					lseverity = ExceptionSeverity.TRANSACTION_SEVERITY;
					break;
				case '2':
					lseverity = ExceptionSeverity.STATEMENT_SEVERITY;
					break;
				}
				break;	
			}
			break;

		default:
			switch (messageID.charAt(6)) {
			case 'M':
				lseverity = ExceptionSeverity.SYSTEM_SEVERITY;
				break;
			case 'D':
				lseverity = ExceptionSeverity.DATABASE_SEVERITY;
				break;
			case 'C':
				lseverity = ExceptionSeverity.SESSION_SEVERITY;
				break;
			case 'T':
				lseverity = ExceptionSeverity.TRANSACTION_SEVERITY;
				break;
			case 'S':
				lseverity = ExceptionSeverity.STATEMENT_SEVERITY;
				break;
			case 'U':
				lseverity = ExceptionSeverity.NO_APPLICABLE_SEVERITY;
				break;
			}
			break;
		}

		return lseverity;
	}

}
