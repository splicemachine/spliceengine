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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.catalog.types.AggregateAliasInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.ClassName;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.compile.AggregateDefinition;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.TypeCompilerFactory;
import com.splicemachine.db.iapi.sql.dictionary.AliasDescriptor;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.JSQLType;

/**
 * Definition for user-defined aggregates.
 *
 */
class UserAggregateDefinition implements AggregateDefinition
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    // the Aggregator interface has 3 parameter types
    private static  final   int INPUT_TYPE = 0;
    private static  final   int RETURN_TYPE = INPUT_TYPE + 1;
    private static  final   int AGGREGATOR_TYPE = RETURN_TYPE + 1;
    private static  final   int AGGREGATOR_PARAM_COUNT = AGGREGATOR_TYPE + 1;

    private static  final   String  DERBY_BYTE_ARRAY_NAME = "byte[]";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private AliasDescriptor _alias;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

	/**
	 * Conjure out of thin air.
	 */
    UserAggregateDefinition( AliasDescriptor alias )
    {
        _alias = alias;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Get the wrapped alias descriptor */
    public  AliasDescriptor getAliasDescriptor() { return _alias; }

	/**
	 * Determines the result datatype and verifies that the input datatype is correct.
	 *
	 * @param inputType	the input type
	 * @param aggregatorClass (Output arg) the name of the Derby execution-time class which wraps the aggregate logic
	 *
	 * @return the result type of the user-defined aggregator
	 */
	public final DataTypeDescriptor	getAggregator
        ( DataTypeDescriptor inputType, StringBuffer aggregatorClass )
        throws StandardException
	{
		try
		{
			CompilerContext cc = (CompilerContext)
				ContextService.getContext(CompilerContext.CONTEXT_ID);
            ClassFactory    classFactory = cc.getClassFactory();
            TypeCompilerFactory tcf = cc.getTypeCompilerFactory();

            Class<?>   derbyAggregatorInterface = classFactory.loadApplicationClass( "com.splicemachine.db.agg.Aggregator" );
            Class<?>   userAggregatorClass = classFactory.loadApplicationClass( _alias.getJavaClassName() );

            Class[][]   typeBounds = classFactory.getClassInspector().getTypeBounds
                ( derbyAggregatorInterface, userAggregatorClass );

            if (
                (typeBounds == null) ||
                (typeBounds.length != AGGREGATOR_PARAM_COUNT) ||
                (typeBounds[ INPUT_TYPE ] == null) ||
                (typeBounds[ RETURN_TYPE ] == null)
                )
            {
                throw StandardException.newException
                    (
                     SQLState.LANG_ILLEGAL_UDA_CLASS,
                     _alias.getSchemaName(),
                     _alias.getName(),
                     userAggregatorClass.getName()
                     );
            }

            Class<?>[] genericParameterTypes =
                classFactory.getClassInspector().getGenericParameterTypes(
                    derbyAggregatorInterface, userAggregatorClass);

            if ( genericParameterTypes == null ) {
                genericParameterTypes = new Class<?>[ AGGREGATOR_PARAM_COUNT ];
            }

            AggregateAliasInfo  aai = (AggregateAliasInfo) _alias.getAliasInfo();
            DataTypeDescriptor  expectedInputType = DataTypeDescriptor.getType( aai.getForType() );
            DataTypeDescriptor  expectedReturnType = DataTypeDescriptor.getType( aai.getReturnType() );
            Class<?>       expectedInputClass = getJavaClass( classFactory, expectedInputType );
            Class<?>       expectedReturnClass = getJavaClass( classFactory, expectedReturnType );

            // the input operand must be coercible to the expected input type of the aggregate
            if ( !tcf.getTypeCompiler( expectedInputType.getTypeId() ).storable( inputType.getTypeId(), classFactory ) )
            { return null; }
            
            //
            // Make sure that the declared input type of the UDA actually falls within
            // the type bounds of the Aggregator implementation.
            //
            Class[] inputBounds = typeBounds[ INPUT_TYPE ];
            for (Class inputBound : inputBounds) {
                vetCompatibility
                        ((Class<?>) inputBound, expectedInputClass, SQLState.LANG_UDA_WRONG_INPUT_TYPE);
            }
            if ( genericParameterTypes[ INPUT_TYPE ] != null )
            {
                vetCompatibility
                    ( genericParameterTypes[ INPUT_TYPE ], expectedInputClass, SQLState.LANG_UDA_WRONG_INPUT_TYPE );
            }

            //
            // Make sure that the declared return type of the UDA actually falls within
            // the type bounds of the Aggregator implementation.
            //
            Class[] returnBounds = typeBounds[ RETURN_TYPE ];
            for (Class returnBound : returnBounds) {
                vetCompatibility
                        (returnBound, expectedReturnClass, SQLState.LANG_UDA_WRONG_RETURN_TYPE);
            }
            if ( genericParameterTypes[ RETURN_TYPE ] != null )
            {
                vetCompatibility
                    ( genericParameterTypes[ RETURN_TYPE ], expectedReturnClass, SQLState.LANG_UDA_WRONG_RETURN_TYPE );
            }

            aggregatorClass.append( ClassName.UserDefinedAggregator );

            return expectedReturnType;
		}
		catch (ClassNotFoundException cnfe) { throw aggregatorInstantiation( cnfe ); }
	}

    /**
     * Verify that an actual type is compatible with the expected type.
     */
    private void    vetCompatibility( Class<?> actualClass, Class<?> expectedClass, String sqlState )
        throws StandardException
    {
        if ( !actualClass.isAssignableFrom( expectedClass ) )
        {
            throw StandardException.newException
                (
                 sqlState,
                 _alias.getSchemaName(),
                 _alias.getName(),
                 expectedClass.toString(),
                 actualClass.toString()
                 );
        }
    }

	/**
	 * Wrap the input operand in an implicit CAST node as necessary in order to
     * coerce it the correct type for the aggregator. Return null if no cast is necessary.
	 */
    final ValueNode castInputValue
        ( ValueNode inputValue, ContextManager cm )
        throws StandardException
	{
        AggregateAliasInfo  aai = (AggregateAliasInfo) _alias.getAliasInfo();
        DataTypeDescriptor  expectedInputType = DataTypeDescriptor.getType( aai.getForType() );
        DataTypeDescriptor  actualInputType = inputValue.getTypeServices();

        // no cast needed if the types match exactly
        if ( expectedInputType.isExactTypeAndLengthMatch( actualInputType ) ) { return null; }
        else
        {
            return StaticMethodCallNode.makeCast(
                inputValue, expectedInputType, cm);
        }
    }
    
    /**
     * Get the Java class corresponding to a Derby datatype.
     */
    private Class<?> getJavaClass( ClassFactory classFactory, DataTypeDescriptor dtd )
        throws StandardException, ClassNotFoundException
    {
        JSQLType    jsqlType = new JSQLType( dtd );
        String  javaClassName = MethodCallNode.getObjectTypeName( jsqlType, null );

        //
        // The real class name of byte[] is [B. Class.forName( "byte[]" ) will throw a
        // ClassNotFoundException.
        //
        if ( DERBY_BYTE_ARRAY_NAME.equals( javaClassName ) )
        { javaClassName = byte[].class.getName(); }
        
        return classFactory.loadApplicationClass( javaClassName );
    }

    /**
     * Make a "Could not instantiate aggregator" exception.
     */
    private StandardException   aggregatorInstantiation( Throwable t )
    {
        return StandardException.newException
            (
             SQLState.LANG_UDA_INSTANTIATION,
             t,
             _alias.getJavaClassName(),
             _alias.getSchemaName(),
             _alias.getName(),
             t.getMessage()
             );
    }

    
}
