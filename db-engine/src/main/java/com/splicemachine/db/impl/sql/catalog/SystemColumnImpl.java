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

import java.sql.Types;

import com.splicemachine.db.iapi.sql.dictionary.SystemColumn;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.iapi.error.StandardException;

/**
 * Implements the description of a column in a system table.
 *
 *
 * @version 0.1
 */

public class SystemColumnImpl implements SystemColumn
{
				private	final String	name;

				/**
				 * Fully described type of the column.
				 */
				private final DataTypeDescriptor type;

				/**
				 * Create a system column for a builtin type.
				 * 
				 * @param name
				 *            name of column
				 * @param jdbcTypeId
				 *            JDBC type id from java.sql.Types
				 * @param nullability
				 *            Whether or not column accepts nulls.
				 */
				public static SystemColumn getColumn(String name, int jdbcTypeId,
												boolean nullability) {
								return new SystemColumnImpl(name, DataTypeDescriptor
																.getBuiltInDataTypeDescriptor(jdbcTypeId, nullability));
				}

				/**
				 * Create a system column for a builtin type.
				 * 
				 * @param name
				 *            name of column
				 * @param jdbcTypeId
				 *            JDBC type id from java.sql.Types
				 * @param nullability
				 *            Whether or not column accepts nulls.
				 * @param maxLength
				 * 						The maximum length of the column
				 */
				public static SystemColumn getColumn(String name, int jdbcTypeId,
												boolean nullability,int maxLength) {
								return new SystemColumnImpl(name, DataTypeDescriptor
																.getBuiltInDataTypeDescriptor(jdbcTypeId, nullability, maxLength));
				}

				/**
				 * Create a system column for an identifer with consistent type of
				 * VARCHAR(128)
				 * 
				 * @param name
				 *            Name of the column.
				 * @param nullability
				 *            Nullability of the column.
				 * @return Object representing the column.
				 */
				public static SystemColumn getIdentifierColumn(String name, boolean nullability) {
								return new SystemColumnImpl(name, DataTypeDescriptor
																.getBuiltInDataTypeDescriptor(Types.VARCHAR, nullability, 128));
				}

				/**
				 * Create a system column for a character representation of a UUID with
				 * consistent type of CHAR(36)
				 * 
				 * @param name
				 *            Name of the column.
				 * @param nullability
				 *            Nullability of the column.
				 * @return Object representing the column.
				 */
				public static SystemColumn getUUIDColumn(String name, boolean nullability) {
								return new SystemColumnImpl(name, DataTypeDescriptor
																.getBuiltInDataTypeDescriptor(Types.CHAR, nullability, 36));
				}

				/**
				 * Create a system column for a character representation of an indicator
				 * column with consistent type of CHAR(1) NOT NULL
				 * 
				 * @param name
				 *            Name of the column.
				 * @return Object representing the column.
				 */
				static SystemColumn getIndicatorColumn(String name) {
								return new SystemColumnImpl(name, DataTypeDescriptor
																.getBuiltInDataTypeDescriptor(Types.CHAR, false, 1));
				}

				/**
				 * Create a system column for a java column.
				 * 
				 * @param name
				 *            Name of the column.
				 * @param javaClassName
				 * @param nullability
				 *            Nullability of the column.
				 * @return Object representing the column.
				 */
				public static SystemColumn getJavaColumn(String name,String javaClassName,boolean nullability)
                throws StandardException {

								TypeId typeId = TypeId.getUserDefinedTypeId(javaClassName, false);

								DataTypeDescriptor dtd = new DataTypeDescriptor(typeId, nullability);
								return new SystemColumnImpl(name, dtd);
				}

				/**
				 * Create a SystemColumnImpl representing the given name and type.
				 */
				private SystemColumnImpl(String name, DataTypeDescriptor type) {
								this.name = name;
								this.type = type;
				}

				/**
				 * Gets the name of this column.
				 *
				 * @return	The column name.
				 */
				public String	getName()
				{
								return	name;
				}

				/**
				 * Return the type of this column.
				 */
				public DataTypeDescriptor getType() {
								return type;
				}
}

