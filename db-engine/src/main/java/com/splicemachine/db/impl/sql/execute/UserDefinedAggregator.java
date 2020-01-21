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

package com.splicemachine.db.impl.sql.execute;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.io.ObjectOutputStream;

import com.splicemachine.db.agg.Aggregator;

import com.splicemachine.db.iapi.services.monitor.Monitor;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecAggregator;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

import com.splicemachine.db.iapi.services.i18n.MessageService;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.reference.MessageId;
import com.splicemachine.db.shared.common.udt.UDTBase;

/**
	Aggregator for user-defined aggregates. Wraps the application-supplied
    implementation of com.splicemachine.db.agg.Aggregator.
 */
public final class UserDefinedAggregator  extends UDTBase implements ExecAggregator
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static final int FIRST_VERSION = 0;
    private static final int SECOND_VERSION = 1; // use java serialization for _aggregator since it will not be registered in our Kryo registry

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private Aggregator  _aggregator;
    private DataTypeDescriptor  _resultType;
    private boolean     _eliminatedNulls;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** 0-arg constructor for Formatable interface */
    public  UserDefinedAggregator() {}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ExecAggregator BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

		public ExecAggregator setup( ClassFactory classFactory, String aggregateName, DataTypeDescriptor resultType )
		{
				try {
						setup( classFactory.loadApplicationClass( aggregateName ), resultType );
				}
				catch (ClassNotFoundException cnfe) { logAggregatorInstantiationError( aggregateName, cnfe ); }
				return this;
		}

    /** Initialization logic shared by setup() and newAggregator() */
    private void    setup( Class udaClass, DataTypeDescriptor resultType )
    {
        String  aggregateName = udaClass.getName();
        
        try {
            _aggregator = (Aggregator) udaClass.newInstance();
            _aggregator.init();
        }
        catch (InstantiationException | IllegalAccessException ie) { logAggregatorInstantiationError( aggregateName, ie ); }

        _resultType = resultType;
    }

	public boolean didEliminateNulls() { return _eliminatedNulls; }

    @SuppressWarnings("unchecked")
	public void accumulate( DataValueDescriptor addend, Object ga ) 
		throws StandardException
	{
		if ( (addend == null) || addend.isNull() )
        {
			_eliminatedNulls = true;
			return;
		}

        Object  value = addend.getObject();

        _aggregator.accumulate( value );
	}

		@SuppressWarnings("unchecked")
		public void merge(ExecAggregator addend)
						throws StandardException
		{
				if(addend==null) return; //ignore null entries
				UserDefinedAggregator  other = (UserDefinedAggregator) addend;

				_aggregator.merge( other._aggregator );
		}

	/**
	 * Return the result of the aggregation. .
	 *
	 * @return the aggregated result (could be a Java null).
	 */
	public DataValueDescriptor getResult() throws StandardException
	{
        Object  javaReturnValue = _aggregator.terminate();
        DataValueDescriptor dvd = _resultType.getNull();
        if ( javaReturnValue == null ) {
        	return dvd;
        }

        dvd.setObjectForCast( javaReturnValue, true, javaReturnValue.getClass().getName() );

        return dvd;
	}

	/**
	 */
	public ExecAggregator newAggregator()
	{
		UserDefinedAggregator   uda = new UserDefinedAggregator();

        uda.setup( _aggregator.getClass(), _resultType );

        return uda;
	}

	/////////////////////////////////////////////////////////////
	// 
	// FORMATABLE INTERFACE
	// 
	/////////////////////////////////////////////////////////////

	/** 
	 *
	 * @exception IOException on error
	 */
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeInt( SECOND_VERSION );
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		oos.writeObject( _aggregator);
		oos.flush();
		byte [] bytes = baos.toByteArray();
		oos.close();
		out.writeInt(bytes.length);
		out.write(baos.toByteArray());
        out.writeObject( _resultType );
        out.writeBoolean( _eliminatedNulls );
	}

	/** 
	 * @see java.io.Externalizable#readExternal 
	 *
	 * @exception IOException on error
	 */
	public void readExternal(ObjectInput in) 
		throws IOException, ClassNotFoundException
	{
		int v = in.readInt();
		if (v == FIRST_VERSION) {
			_aggregator = (Aggregator) in.readObject();
		} else {
			byte[] bytes = new byte[in.readInt()];
			if (bytes.length > 0) {
				in.readFully(bytes);
				ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
				_aggregator = (Aggregator) ois.readObject();
				ois.close();
			}
		}
        _resultType = (DataTypeDescriptor) in.readObject();
        _eliminatedNulls = in.readBoolean();
	}

	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.AGG_USER_ADAPTOR_V01_ID; }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * Record an instantiation error trying to load the aggregator class.
     */
    private void   logAggregatorInstantiationError( String aggregateName, Throwable t )
    {
        String  errorMessage = MessageService.getTextMessage
            (
             MessageId.CM_CANNOT_LOAD_CLASS,
             aggregateName,
             t.getMessage()
             );

		Monitor.getStream().printThrowable( errorMessage , t);
    }

    @Override
    public void add(DataValueDescriptor addend) throws StandardException {
        throw new UnsupportedOperationException();
    }

    public Aggregator getAggregator() {
        return _aggregator;
    }

	@Override
	public boolean isUserDefinedAggregator() {
		return true;
	}

}
