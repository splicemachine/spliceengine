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

package com.splicemachine.db.iapi.sql.conn;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.db.Database;

import com.splicemachine.db.iapi.services.property.PropertyFactory;

import com.splicemachine.db.iapi.sql.compile.OptimizerFactory;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;

import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.sql.compile.TypeCompilerFactory;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.sql.Statement;
import com.splicemachine.db.iapi.sql.compile.Parser;

import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.services.compiler.JavaFactory;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.context.ContextManager;

import com.splicemachine.db.iapi.sql.LanguageFactory;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;


/**
 * Factory interface for items specific to a connection in the language system.
 * This is expected to be used internally, and so is not in Language.Interface.
 * <p>
 * This Factory provides pointers to other language factories; the
 * LanguageConnectionContext holds more dynamic information, such as
 * prepared statements and whether a commit has occurred or not.
 * <p>
 * This Factory is for internal items used throughout language during a
 * connection. Things that users need for the Database API are in
 * LanguageFactory in Language.Interface.
 * <p>
 * This factory returns (and thus starts) all the other per-database
 * language factories. So there might someday be properties as to which
 * ones to start (attributes, say, like level of optimization).
 * If the request is relative to a specific connection, the connection
 * is passed in. Otherwise, they are assumed to be database-wide services.
 *
 * @see com.splicemachine.db.iapi.sql.LanguageFactory
 *
 */
public interface LanguageConnectionFactory {
	/**
		Used to locate this factory by the Monitor basic service.
		There needs to be a language factory per database.
	 */
	String MODULE = "com.splicemachine.db.iapi.sql.conn.LanguageConnectionFactory";


	/**
		Get a Statement
		@param compilationSchema schema
		@param statementText the text for the statement
		@param forReadOnly true if concurrency mode is CONCUR_READ_ONLY
		@return	The Statement
	 */
	Statement getStatement(SchemaDescriptor compilationSchema, String statementText, boolean forReadOnly);

	/**
		Get a new LanguageConnectionContext. this holds things
		we want to remember about activity in the language system,
		where this factory holds things that are pretty stable,
		like other factories.
		<p>
		The returned LanguageConnectionContext is intended for use
		only by the connection that requested it.

		@return a language connection context for the context stack.
		@exception StandardException the usual
	 */
	LanguageConnectionContext
	newLanguageConnectionContext(ContextManager cm,
								TransactionController tc,
								LanguageFactory lf,
								Database db,
								String userName,
								String drdaID,
								String dbname,
                                CompilerContext.DataSetProcessorType type)

		throws StandardException;

	/**
		Get the UUIDFactory to use with this language connection
	 */
	UUIDFactory	getUUIDFactory();

	/**
		Get the ClassFactory to use with this language connection
	 */
	ClassFactory	getClassFactory();

	/**
		Get the JavaFactory to use with this language connection
	 */
	JavaFactory	getJavaFactory();

	/**
		Get the NodeFactory to use with this language connection
	 */
	NodeFactory	getNodeFactory();

	/**
		Get the ExecutionFactory to use with this language connection
	 */
	ExecutionFactory	getExecutionFactory();

	/**
		Get the PropertyFactory to use with this language connection
	 */
	PropertyFactory	getPropertyFactory();

	/**
		Get the OptimizerFactory to use with this language connection
	 */
	OptimizerFactory	getOptimizerFactory();

	/**
		Get the TypeCompilerFactory to use with this language connection
	 */
	TypeCompilerFactory getTypeCompilerFactory();

	/**
		Get the DataValueFactory to use with this language connection
		This is expected to get stuffed into the language connection
		context and accessed from there.

	 */
	DataValueFactory		getDataValueFactory();



    public Parser newParser(CompilerContext cc);
}
