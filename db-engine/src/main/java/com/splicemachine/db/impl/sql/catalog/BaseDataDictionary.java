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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.monitor.ModuleControl;
import com.splicemachine.db.iapi.services.monitor.ModuleSupportable;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;

public abstract class BaseDataDictionary implements DataDictionary, ModuleControl, ModuleSupportable,java.security.PrivilegedAction {
	protected static final String		CFG_SYSTABLES_ID = "SystablesIdentifier";
	protected static final String		CFG_SYSTABLES_INDEX1_ID = "SystablesIndex1Identifier";
	protected static final String		CFG_SYSTABLES_INDEX2_ID = "SystablesIndex2Identifier";
	protected static final String		CFG_SYSCOLUMNS_ID = "SyscolumnsIdentifier";
	protected static final String		CFG_SYSCOLUMNS_INDEX1_ID = "SyscolumnsIndex1Identifier";
	protected static final String		CFG_SYSCOLUMNS_INDEX2_ID = "SyscolumnsIndex2Identifier";
	protected static final String		CFG_SYSCONGLOMERATES_ID = "SysconglomeratesIdentifier";
	protected static final String		CFG_SYSCONGLOMERATES_INDEX1_ID = "SysconglomeratesIndex1Identifier";
	protected static final String		CFG_SYSCONGLOMERATES_INDEX2_ID = "SysconglomeratesIndex2Identifier";
	protected static final String		CFG_SYSCONGLOMERATES_INDEX3_ID = "SysconglomeratesIndex3Identifier";
	protected static final String		CFG_SYSSCHEMAS_ID = "SysschemasIdentifier";
	protected static final String		CFG_SYSSCHEMAS_INDEX1_ID = "SysschemasIndex1Identifier";
	protected static final String		CFG_SYSSCHEMAS_INDEX2_ID = "SysschemasIndex2Identifier";
	protected	static final int		SYSCONGLOMERATES_CORE_NUM = 0;
	protected	static final int		SYSTABLES_CORE_NUM = 1;
	protected static final int		SYSCOLUMNS_CORE_NUM = 2;
	protected	static final int		SYSSCHEMAS_CORE_NUM = 3;
	protected static final int        NUM_CORE = 4;
	/**
	* SYSFUN functions. Table of functions that automatically appear
	* in the SYSFUN schema. These functions are resolved to directly
	* if no schema name is given, e.g.
	* 
	* <code>
	* SELECT COS(angle) FROM ROOM_WALLS
	* </code>
	* 
	* Adding a function here is suitable when the function defintion
	* can have a single return type and fixed parameter types.
	* 
	* Functions that need to have a return type based upon the
	* input type(s) are not supported here. Typically those are
	* added into the parser and methods added into the DataValueDescriptor interface.
	* Examples are character based functions whose return type
	* length is based upon the passed in type, e.g. passed a CHAR(10)
	* returns a CHAR(10).
	* 
	* 
    * This simple table can handle an arbitrary number of arguments
	* and RETURNS NULL ON NULL INPUT. The scheme could be expanded
	* to handle other function options such as other parameters if needed.
    *[0]    = FUNCTION name
    *[1]    = RETURNS type
    *[2]    = Java class
    *[3]    = method name and signature
    *[4]    = "true" or "false" depending on whether the function is DETERMINSTIC
    *[5..N] = arguments (optional, if not present zero arguments is assumed)
	*
	*/
	protected static final String[][] SYSFUN_FUNCTIONS = {
        {"ACOS", "DOUBLE", "java.lang.StrictMath", "acos(double)", "true", "DOUBLE"},
			{"ASIN", "DOUBLE", "java.lang.StrictMath", "asin(double)",  "true", "DOUBLE"},
			{"ATAN", "DOUBLE", "java.lang.StrictMath", "atan(double)",  "true", "DOUBLE"},
            {"ATAN2", "DOUBLE", "java.lang.StrictMath", "atan2(double,double)",  "true", "DOUBLE", "DOUBLE"},
			{"COS", "DOUBLE", "java.lang.StrictMath", "cos(double)",  "true", "DOUBLE"},
			{"SIN", "DOUBLE", "java.lang.StrictMath", "sin(double)",  "true", "DOUBLE"},
			{"TAN", "DOUBLE", "java.lang.StrictMath", "tan(double)",  "true", "DOUBLE"},
            {"PI", "DOUBLE", "com.splicemachine.db.catalog.SystemProcedures", "PI()", "true"},
            {"DEGREES", "DOUBLE", "java.lang.StrictMath", "toDegrees(double)", "true", "DOUBLE"},
			{"RADIANS", "DOUBLE", "java.lang.StrictMath", "toRadians(double)",  "true", "DOUBLE"},
			{"LN", "DOUBLE", "java.lang.StrictMath", "log(double)",  "true", "DOUBLE"},
			{"LOG", "DOUBLE", "java.lang.StrictMath", "log(double)",  "true", "DOUBLE"}, // Same as LN
			{"LOG10", "DOUBLE", "com.splicemachine.db.catalog.SystemProcedures", "LOG10(double)",  "true", "DOUBLE"},
			{"EXP", "DOUBLE", "java.lang.StrictMath", "exp(double)",  "true", "DOUBLE"},
			{"CEIL", "DOUBLE", "java.lang.StrictMath", "ceil(double)",  "true", "DOUBLE"},
			{"CEILING", "DOUBLE", "java.lang.StrictMath", "ceil(double)",  "true", "DOUBLE"}, // Same as CEIL
			{"FLOOR", "DOUBLE", "java.lang.StrictMath", "floor(double)",  "true", "DOUBLE"},
			{"SIGN", "INTEGER", "com.splicemachine.db.catalog.SystemProcedures", "SIGN(double)",  "true", "DOUBLE"},
            {"RANDOM", "DOUBLE", "java.lang.StrictMath", "random()",  "false" },
			{"RAND", "DOUBLE", "com.splicemachine.db.catalog.SystemProcedures", "RAND(int)",  "false", "INTEGER"}, // Escape function spec.
			{"COT", "DOUBLE", "com.splicemachine.db.catalog.SystemProcedures", "COT(double)",  "true", "DOUBLE"},
			{"COSH", "DOUBLE", "com.splicemachine.db.catalog.SystemProcedures", "COSH(double)",  "true", "DOUBLE"},
			{"SINH", "DOUBLE", "com.splicemachine.db.catalog.SystemProcedures", "SINH(double)",  "true", "DOUBLE"},
			{"TANH", "DOUBLE", "com.splicemachine.db.catalog.SystemProcedures", "TANH(double)",  "true", "DOUBLE"}
	};


    /**
     * Index into SYSFUN_FUNCTIONS of the DETERMINISTIC indicator.
     * Used to determine whether the system function is DETERMINISTIC
     */
    protected static final int SYSFUN_DETERMINISTIC_INDEX =  4;

    /**
     * The index of the first parameter in entries in the SYSFUN_FUNCTIONS
     * table. Used to determine the parameter count (zero to many).
     */
    protected static final int SYSFUN_FIRST_PARAMETER_INDEX =  5;

	// This array of non-core table names *MUST* be in the same order
	// as the non-core table numbers in DataDictionary.
	protected static final String[] nonCoreNames = {
									"SYSCONSTRAINTS",
									"SYSKEYS",
									"SYSPRIMARYKEYS",
									"SYSDEPENDS",
									"SYSALIASES",
									"SYSVIEWS",
									"SYSCHECKS",
									"SYSFOREIGNKEYS",
									"SYSSTATEMENTS",
									"SYSFILES",
									"SYSTRIGGERS",
                                    "SYSTABLEPERMS",
                                    "SYSSCHEMAPERMS",
                                    "SYSCOLPERMS",
                                    "SYSROUTINEPERMS",
									"SYSROLES",
                                    "SYSSEQUENCES",
                                    "SYSPERMS",
                                    "SYSUSERS",
                                    "SYSBACKUP",
                                    "SYSBACKUPFILESET",
                                    "SYSBACKUPITEMS",
                                    "SYSBACKUPJOBS",
                                    "SYSCOLUMNSTATS",
                                    "SYSPHYSICALSTATS",
                                    "SYSTABLESTATS",
                                    "SYSDUMMY1"
    };

	protected	static final int		NUM_NONCORE = nonCoreNames.length;
	
    /**
     * List of all "system" schemas
     * <p>
     * This list should contain all schema's used by the system and are
     * created when the database is created.  Users should not be able to
     * create or drop these schema's and should not be able to create or
     * drop objects in these schema's.  This list is used by code that
     * needs to check if a particular schema is a "system" schema.
     **/
    protected static final String[] systemSchemaNames = {
        SchemaDescriptor.IBM_SYSTEM_CAT_SCHEMA_NAME,
        SchemaDescriptor.IBM_SYSTEM_FUN_SCHEMA_NAME,
        SchemaDescriptor.IBM_SYSTEM_PROC_SCHEMA_NAME,
        SchemaDescriptor.IBM_SYSTEM_STAT_SCHEMA_NAME,
        SchemaDescriptor.IBM_SYSTEM_NULLID_SCHEMA_NAME,
        SchemaDescriptor.STD_SYSTEM_DIAG_SCHEMA_NAME,
        SchemaDescriptor.STD_SYSTEM_UTIL_SCHEMA_NAME,
        SchemaDescriptor.IBM_SYSTEM_SCHEMA_NAME,
        SchemaDescriptor.STD_SQLJ_SCHEMA_NAME,
        SchemaDescriptor.STD_SYSTEM_SCHEMA_NAME
    };

	/**
	 * List of procedures in SYSCS_UTIL schema with PUBLIC access
	 */
	protected static final String[] sysUtilProceduresWithPublicAccess = { 
												"SYSCS_INPLACE_COMPRESS_TABLE",
												"SYSCS_COMPRESS_TABLE",
												"SYSCS_MODIFY_PASSWORD",
												};
	
	/**
	 * List of functions in SYSCS_UTIL schema with PUBLIC access
	 */
	protected static final String[] sysUtilFunctionsWithPublicAccess = { 
												"SYSCS_PEEK_AT_SEQUENCE",
												};


	@Override
	public void startWriting(LanguageConnectionContext lcc,boolean setDDMode) throws StandardException{
		/*
		 * By default, ignore the set dd mode flag and just assume it's true. This adheres to traditional
		 * derby behavior
		 */
		startWriting(lcc);
	}

	@Override
	public boolean canUseSPSCache() throws StandardException{
		return canUseCache(null);
	}
}
