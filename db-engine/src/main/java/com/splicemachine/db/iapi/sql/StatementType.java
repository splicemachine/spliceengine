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

package com.splicemachine.db.iapi.sql;

/**
 * Different types of statements
 *
 */
public interface StatementType
{
	public static final int UNKNOWN	= 0;
	public static final int INSERT	= 1;
	public static final int BULK_INSERT_REPLACE = 2;
	public static final int UPDATE	= 3;
	public static final int DELETE	= 4;
	public static final int ENABLED = 5;
	public static final int DISABLED = 6;

	public static final int DROP_CASCADE = 0;
	public static final int DROP_RESTRICT = 1;
    public static final int DROP_DEFAULT = 2;
    public static final int DROP_IF_EXISTS = 3;

	public static final int RENAME_TABLE = 1;
	public static final int RENAME_COLUMN = 2;
	public static final int RENAME_INDEX = 3;

	public static final int RA_CASCADE = 0;
	public static final int RA_RESTRICT = 1;
	public static final int RA_NOACTION = 2;  //default value
	public static final int RA_SETNULL = 3;
	public static final int RA_SETDEFAULT = 4;
	
	public static final int SET_SCHEMA_USER = 1;
	public static final int SET_SCHEMA_DYNAMIC = 2;

    public static final int SET_ROLE_DYNAMIC = 1;	
}





