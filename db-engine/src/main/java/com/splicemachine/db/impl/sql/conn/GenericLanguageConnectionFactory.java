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

package com.splicemachine.db.impl.sql.conn;

import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.Module;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionFactory;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.LanguageFactory;
import com.splicemachine.db.impl.sql.GenericStatement;
import com.splicemachine.db.iapi.services.uuid.UUIDFactory;
import com.splicemachine.db.iapi.services.compiler.JavaFactory;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.db.Database;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.sql.compile.TypeCompilerFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.NodeFactory;
import com.splicemachine.db.iapi.sql.compile.Parser;
import com.splicemachine.db.iapi.services.property.PropertyFactory;
import com.splicemachine.db.iapi.sql.Statement;
import com.splicemachine.db.iapi.sql.compile.OptimizerFactory;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.monitor.ModuleControl;
import com.splicemachine.db.iapi.services.monitor.ModuleSupportable;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.cache.CacheManager;
import com.splicemachine.db.iapi.services.cache.CacheableFactory;
import com.splicemachine.db.iapi.services.cache.Cacheable;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.services.property.PropertySetCallback;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.reference.EngineType;
import java.util.Properties;
import java.util.Dictionary;
import java.io.Serializable;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.db.iapi.services.daemon.Serviceable;
import com.splicemachine.db.iapi.util.StringUtil;

/**
 * LanguageConnectionFactory generates all of the items
 * a language system needs that is specific to a particular
 * connection. Alot of these are other factories.
 *
 */
public class GenericLanguageConnectionFactory
	implements LanguageConnectionFactory, CacheableFactory, PropertySetCallback, ModuleControl, ModuleSupportable {

	/*
		fields
	 */
	private 	ExecutionFactory		ef;
	private 	OptimizerFactory		of;
	private		TypeCompilerFactory		tcf;
	private 	DataValueFactory		dvf;
	private 	UUIDFactory				uuidFactory;
	private 	JavaFactory				javaFactory;
	private 	ClassFactory			classFactory;
	private 	NodeFactory				nodeFactory;
	private 	PropertyFactory			pf;

	private		int						nextLCCInstanceNumber;

	/*
	  for caching prepared statements 
	*/
	private int cacheSize = Property.STATEMENT_CACHE_SIZE_DEFAULT;

	/*
	   constructor
	*/
	public GenericLanguageConnectionFactory() {
	}

	/*
	   LanguageConnectionFactory interface
	*/

	/*
		these are the methods that do real work, not just look for factories
	 */

	/**
		Get a Statement for the connection
		@param compilationSchema schema
		@param statementText the text for the statement
		@param forReadOnly if concurrency is CONCUR_READ_ONLY
		@return	The Statement
	 */
        public Statement getStatement(SchemaDescriptor compilationSchema, String statementText, boolean forReadOnly)
        {
	    return new GenericStatement(compilationSchema, statementText, forReadOnly);
	}


	/**
		Get a LanguageConnectionContext. this holds things
		we want to remember about activity in the language system,
		where this factory holds things that are pretty stable,
		like other factories.
		<p>
		The returned LanguageConnectionContext is intended for use
		only by the connection that requested it.

		@return a language connection context for the context stack.
		@exception StandardException the usual -- for the subclass
	 */
    @Override
	public LanguageConnectionContext newLanguageConnectionContext(
		ContextManager cm,
		TransactionController tc,
		LanguageFactory lf,
		Database db,
		String userName,
		String drdaID,
		String dbname,
        CompilerContext.DataSetProcessorType type) throws StandardException {
		
		return new GenericLanguageConnectionContext(cm,
													tc,
													lf,
													this,
													db,
													userName,
													getNextLCCInstanceNumber(),
													drdaID,
													dbname,
                                                    type);
	}

	public Cacheable newCacheable(CacheManager cm) {
		return new CachedStatement();
	}

	/*
		these methods all look for factories that we booted.
	 */
	 
	 /**
		Get the UUIDFactory to use with this language connection
		REMIND: this is only used by the compiler; should there be
		a compiler module control class to boot compiler-only stuff?
	 */
	public UUIDFactory	getUUIDFactory()
	{
		return uuidFactory;
	}

	/**
		Get the ClassFactory to use with this language connection
	 */
	public ClassFactory	getClassFactory()
	{
		return classFactory;
	}

	/**
		Get the JavaFactory to use with this language connection
		REMIND: this is only used by the compiler; should there be
		a compiler module control class to boot compiler-only stuff?
	 */
	public JavaFactory	getJavaFactory()
	{
		return javaFactory;
	}

	/**
		Get the NodeFactory to use with this language connection
		REMIND: is this only used by the compiler?
	 */
	public NodeFactory	getNodeFactory()
	{
		return nodeFactory;
	}

	/**
		Get the ExecutionFactory to use with this language connection
	 */
	public ExecutionFactory	getExecutionFactory() {
		return ef;
	}

	/**
		Get the PropertyFactory to use with this language connection
	 */
	public PropertyFactory	getPropertyFactory() 
	{
		return pf;
	}	

	/**
		Get the OptimizerFactory to use with this language connection
	 */
	public OptimizerFactory	getOptimizerFactory() {
		return of;
	}
	/**
		Get the TypeCompilerFactory to use with this language connection
	 */
	public TypeCompilerFactory getTypeCompilerFactory() {
		return tcf;
	}

	/**
		Get the DataValueFactory to use with this language connection
	 */
	public DataValueFactory		getDataValueFactory() {
		return dvf;
	}

	/*
		ModuleControl interface
	 */

	/**
		this implementation will not support caching of statements.
	 */
    @Override
	public boolean canSupport(Properties startParams) {
		return Monitor.isDesiredType(startParams, EngineType.STANDALONE_DB);
	}

	private	int	statementCacheSize(Properties startParams)
	{
		String wantCacheProperty = null;

		wantCacheProperty =
			PropertyUtil.getPropertyFromSet(startParams, Property.STATEMENT_CACHE_SIZE);

		if (SanityManager.DEBUG)
			SanityManager.DEBUG("StatementCacheInfo", "Cacheing implementation chosen if null or 0<"+wantCacheProperty);

		if (wantCacheProperty != null) {
			try {
			    cacheSize = Integer.parseInt(wantCacheProperty);
			} catch (NumberFormatException nfe) {
				cacheSize = Property.STATEMENT_CACHE_SIZE_DEFAULT;
			}
		}

		return cacheSize;
	}
	
	/**
	 * Start-up method for this instance of the language connection factory.
	 * Note these are expected to be booted relative to a Database.
	 *
	 * @param startParams	The start-up parameters (ignored in this case)
	 *
	 * @exception StandardException	Thrown on failure to boot
	 */
	public void boot(boolean create, Properties startParams) 
		throws StandardException {

		//The following call to Monitor to get DVF is going to get the already
		//booted DVF (DVF got booted by BasicDatabase's boot method. 
		//BasicDatabase also set the correct Locale in the DVF. There after,
		//DVF with correct Locale is available to rest of the Derby code.
		dvf = (DataValueFactory) Monitor.bootServiceModule(create, this, ClassName.DataValueFactory, startParams);
		javaFactory = (JavaFactory) Monitor.startSystemModule(Module.JavaFactory);
		uuidFactory = Monitor.getMonitor().getUUIDFactory();
		classFactory = (ClassFactory) Monitor.getServiceModule(this, Module.ClassFactory);
		if (classFactory == null)
 			classFactory = (ClassFactory) Monitor.findSystemModule(Module.ClassFactory);

		//set the property validation module needed to do propertySetCallBack
		//register and property validation
		setValidation();

		ef = (ExecutionFactory) Monitor.bootServiceModule(create, this, ExecutionFactory.MODULE, startParams);
		of = (OptimizerFactory) Monitor.bootServiceModule(create, this, OptimizerFactory.MODULE, startParams);
		tcf =
		   (TypeCompilerFactory) Monitor.startSystemModule(TypeCompilerFactory.MODULE);
		nodeFactory = (NodeFactory) Monitor.bootServiceModule(create, this, NodeFactory.MODULE, startParams);

	}

	/**
	 * Stop this module.  In this case, nothing needs to be done.
	 */
	public void stop() {
	}

	/*
	** Methods of PropertySetCallback
	*/

	public void init(boolean dbOnly, Dictionary p) {
		// not called yet ...
	}

	/**
	  @see PropertySetCallback#validate
	  @exception StandardException Thrown on error.
	*/
	public boolean validate(String key,
						 Serializable value,
						 Dictionary p)
		throws StandardException {
		if (value == null)
			return true;
		else if (key.equals(Property.DEFAULT_CONNECTION_MODE_PROPERTY))
		{
			String value_s = (String)value;
			if (value_s != null &&
				!StringUtil.SQLEqualsIgnoreCase(value_s, Property.NO_ACCESS) &&
				!StringUtil.SQLEqualsIgnoreCase(value_s, Property.READ_ONLY_ACCESS) &&
				!StringUtil.SQLEqualsIgnoreCase(value_s, Property.FULL_ACCESS))
				throw StandardException.newException(SQLState.AUTH_INVALID_AUTHORIZATION_PROPERTY, key, value_s);

			return true;
		}
		else if (key.equals(Property.READ_ONLY_ACCESS_USERS_PROPERTY) ||
				 key.equals(Property.FULL_ACCESS_USERS_PROPERTY))
		{
			String value_s = (String)value;

			/** Parse the new userIdList to verify its syntax. */
			String[] newList_a;
			try {newList_a = IdUtil.parseIdList(value_s);}
			catch (StandardException se) {
                throw StandardException.newException(SQLState.AUTH_INVALID_AUTHORIZATION_PROPERTY, se, key,value_s);
			}

			/** Check the new list userIdList for duplicates. */
			String dups = IdUtil.dups(newList_a);
			if (dups != null) throw StandardException.newException(SQLState.AUTH_DUPLICATE_USERS, key,dups);

			/** Check for users with both read and full access permission. */
			String[] otherList_a;
			String otherList;
			if (key.equals(Property.READ_ONLY_ACCESS_USERS_PROPERTY))
				otherList = (String)p.get(Property.FULL_ACCESS_USERS_PROPERTY);
			else
				otherList = (String)p.get(Property.READ_ONLY_ACCESS_USERS_PROPERTY);
			otherList_a = IdUtil.parseIdList(otherList);
			String both = IdUtil.intersect(newList_a,otherList_a);
			if (both != null) throw StandardException.newException(SQLState.AUTH_USER_IN_READ_AND_WRITE_LISTS, both);
			
			return true;
		}

		return false;
	}
	/** @see PropertySetCallback#apply */
	@Override
	public Serviceable apply(String key,
							 Serializable value,
							 Dictionary p, TransactionController tc) {
			 return null;
	}
	/** @see PropertySetCallback#map */
	public Serializable map(String key, Serializable value, Dictionary p)
	{
		return null;
	}

	protected void setValidation() throws StandardException {
		pf = (PropertyFactory) Monitor.findServiceModule(this,
			Module.PropertyFactory);
		pf.addPropertySetNotification(this);
	}

    public Parser newParser(CompilerContext cc)
    {
        return new com.splicemachine.db.impl.sql.compile.ParserImpl(cc);
    }

	// Class methods

	/**
	 * Get the instance # for the next LCC.
	 * (Useful for logStatementText=true output.
	 *
	 * @return instance # of next LCC.
	 */
	protected synchronized int getNextLCCInstanceNumber()
	{
		return nextLCCInstanceNumber++;
	}
}
