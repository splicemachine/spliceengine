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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.services.compiler.LocalField;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.services.loader.GeneratedClass;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SPSDescriptor;

import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;

import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;

import com.splicemachine.db.impl.services.bytecode.GClass;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;

import com.splicemachine.db.iapi.util.ByteArray;

import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * A ExecSPSNode is the root of a QueryTree 
 * that represents an EXECUTE STATEMENT
 * statement.  It is a tad abnormal.  Duringa
 * bind, it locates and retrieves the SPSDescriptor
 * for the particular statement.  At generate time,
 * it generates the prepared statement for the 
 * stored prepared statement and returns it (i.e.
 * it effectively replaces itself with the appropriate
 * prepared statement).
 *
 */

public class ExecSPSNode extends StatementNode 
{
	private TableName			name;
	private SPSDescriptor		spsd;
	private ExecPreparedStatement ps;

	/**
	 * Initializer for a ExecSPSNode
	 *
	 * @param newObjectName		The name of the table to be created
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void init(
				Object 		newObjectName)
	{
		this.name = (TableName) newObjectName;
	}

	/**
	 * Bind this ExecSPSNode.  This means doing any static error
	 * checking that can be done before actually creating the table.
	 * For example, verifying that the ResultColumnList does not
	 * contain any duplicate column names.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindStatement() throws StandardException
	{
		/*
		** Grab the compiler context each time we bind just
		** to make sure we have the write one (even though
		** we are caching it).
		*/
		DataDictionary dd = getDataDictionary();

		String schemaName = name.getSchemaName();
		SchemaDescriptor sd = getSchemaDescriptor(name.getSchemaName());
		if (schemaName == null)
			name.setSchemaName(sd.getSchemaName());

		if (sd.getUUID() != null)
			spsd = dd.getSPSDescriptor(name.getTableName(), sd);

		if (spsd == null)
		{
			throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND, "STATEMENT", name);
		}

		if (spsd.getType() == spsd.SPS_TYPE_TRIGGER)
		{
			throw StandardException.newException(SQLState.LANG_TRIGGER_SPS_CANNOT_BE_EXECED, name);
		}
		

		/*
		** This execute statement is dependent on the
		** stored prepared statement.  If for any reason
		** the underlying statement is invalidated by
		** the time we get to execution, the 'execute statement'
		** will get invalidated when the underlying statement
		** is invalidated.
		*/
		getCompilerContext().createDependency(spsd);

	}

	/**
	 * SPSes are atomic if its underlying statement is
	 * atomic.
	 *
	 * @return true if the statement is atomic
	 */	
	public boolean isAtomic() {
		if (SanityManager.DEBUG) {
			SanityManager.ASSERT(ps != null, 
				"statement expected to be bound before calling isAtomic()");
		}

		return ps.isAtomic();
	}

	/**
	 * Do code generation for this statement.  Overrides
	 * the normal generation path in StatementNode.
	 *
	 * @param	ignored - ignored (he he)
	 *
	 * @return		A GeneratedClass for this statement
	 *
	 * @exception StandardException		Thrown on error
	 */
	public GeneratedClass generate(ByteArray ignored) throws StandardException
	{
		//Bug 4821 - commiting the nested transaction will release any bind time locks
		//This way we won't get lock time out errors while trying to update sysstatement
		//table during stale sps recompilation later in the getPreparedstatement() call.
		if (spsd.isValid() == false) {
			getLanguageConnectionContext().commitNestedTransaction();
			getLanguageConnectionContext().beginNestedTransaction(true);
		}  

		/*
		** The following does a prepare on the underlying
		** statement if necessary.  The returned statement
		** is valid and its class is loaded up.
		*/
		ps = spsd.getPreparedStatement();


		/*
		** Set the saved constants from the prepared statement.
		** Put them in the compilation context -- this is where
		** they are expected.
		*/
		getCompilerContext().setSavedObjects(ps.getSavedObjects());
		getCompilerContext().setCursorInfo(ps.getCursorInfo());
		GeneratedClass gc = ps.getActivationClass();

		/*
		 * If we can store the bytes, do so, because otherwise serialization
		 * may get all kinds of mucked up.
		 */
		if(ps instanceof GenericStorablePreparedStatement){
		//ugly little hack to ensure that we can write the class file out for ExecSPSNode types
			if(SanityManager.DEBUG && SanityManager.DEBUG_ON("DumpClassFile")){
                String systemHome = (String)AccessController.doPrivileged(new PrivilegedAction() {
					
//                    @Override
                    public Object run() {
                        return System.getProperty(Property.SYSTEM_HOME_PROPERTY, ".");
					}
				});	
				new DumpGClass((GenericStorablePreparedStatement)ps,gc.getName()).writeClassFile(systemHome,false,null);
			}
			//store the byteCode for serialization
			ByteArray source = ((GenericStorablePreparedStatement)ps).getByteCodeSaver();
			ignored.setBytes(source.getArray(), source.getOffset(), source.getLength());
		}
		
		return gc;
	}
		
	/**
	 * Make the result description.  Really, we are just
	 * copying it from the stored prepared statement.
	 *
	 * @return	the description
	 */
	public ResultDescription makeResultDescription()
	{
		return ps.getResultDescription();
	}

	/**
	 * Get information about this cursor.  For sps,
	 * this is info saved off of the original query
	 * tree (the one for the underlying query).
	 *
	 * @return	the cursor info
	 */
	public Object getCursorInfo()
	{
		return ps.getCursorInfo();
	}

	/**
	 * Return a description of the ? parameters for the statement
	 * represented by this query tree.  Just return the params
	 * stored with the prepared statement.
	 *
	 * @return	An array of DataTypeDescriptors describing the
	 *		? parameters for this statement.  It returns null
	 *		if there are no parameters.
	 *
	 * @exception StandardException on error
	 */
	public DataTypeDescriptor[]	getParameterTypes() throws StandardException
	{
		return spsd.getParams();
	}


	/**
	 * Create the Constant information that will drive the guts of Execution.
	 * This is assumed to be the first action on this node.
	 *
	 */
	public ConstantAction	makeConstantAction()
	{
		return ps.getConstantAction();
	}

	/**
	 * We need a savepoint if we will do transactional work.
	 * We'll ask the underlying statement if it needs
	 * a savepoint and pass that back.  We have to do this
	 * after generation because getting the PS now might
	 * cause us to basically do DDL (for a stmt recompilation)
	 * which is explicitly banned during binding.  So the
	 * caller can only call this after generate() has retrieved
	 * the target PS.  
	 *
	 * @return boolean	always true.
	 */
	public boolean needsSavepoint()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(ps != null, 
				"statement expected to be bound before calling needsSavepoint()");
		}

		return ps.needsSavepoint();
	}

	/** @see StatementNode#executeStatementName */
	public String executeStatementName()
	{
		return name.getTableName();
	}

	/** @see StatementNode#executeSchemaName */
	public String executeSchemaName()
	{
		return name.getSchemaName();
	}

	/**
	 * Get the name of the SPS that is used
	 * to execute this statement.  Only relevant
	 * for an ExecSPSNode -- otherwise, returns null.
	 *
	 * @return the name of the underlying sps
	 */
	public String getSPSName()
	{
		return spsd.getQualifiedName();
	}
		
	/*
	 * Shouldn't be called
	 */
	int activationKind()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.THROWASSERT("activationKind not expected "+
				"to be called for a stored prepared statement");
		}
	   return StatementNode.NEED_PARAM_ACTIVATION;
	}
	/////////////////////////////////////////////////////////////////////
	//
	// PRIVATE
	//
	/////////////////////////////////////////////////////////////////////

		
	/////////////////////////////////////////////////////////////////////
	//
	// MISC
	//
	/////////////////////////////////////////////////////////////////////
	public String statementToString()
	{
		return "EXECUTE STATEMENT";
	}

	// called after bind only
	private final SPSDescriptor getSPSDescriptor()
	{
		return spsd;
	}

    private static class DumpGClass extends GClass {
        private final GenericStorablePreparedStatement ps;

        private DumpGClass(GenericStorablePreparedStatement ps,String name) {
            super(null,name);
            this.ps = ps;
            this.bytecode=ps.getByteCodeSaver();
        }

//        @Override
        public LocalField addField(String type, String name, int modifiers) {
            return null; //no-op
        }

//        @Override
        public ByteArray getClassBytecode() throws StandardException {
            return ps.getByteCodeSaver();
        }

//        @Override
        public void writeClassFile(String dir, boolean logMessage, Throwable t) throws StandardException {
            super.writeClassFile(dir, logMessage, t);
        }

//        @Override
        public String getName() {
            return ps.getObjectName();
        }

//        @Override
        public MethodBuilder newMethodBuilder(int modifiers, String returnType, String methodName) {
            return null;
        }

//        @Override
        public MethodBuilder newMethodBuilder(int modifiers, String returnType, String methodName, String[] parms) {
            return null;
        }

//        @Override
        public MethodBuilder newConstructorBuilder(int modifiers) {
            return null;
        }

//        @Override
        public void newFieldWithAccessors(String getter, String setter, int methodModifier, boolean staticField, String type) {
        }

		@Override
		public boolean existsField(String javaType, String name) {return false;}
    }
}
