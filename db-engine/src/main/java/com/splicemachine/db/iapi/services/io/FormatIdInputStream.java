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

package com.splicemachine.db.iapi.services.io;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.StreamCorruptedException;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.services.loader.ClassFactoryContext;
import com.splicemachine.db.iapi.types.Resetable;

import com.splicemachine.db.iapi.services.context.ContextService;
/**
  A stream for reading objects with format id tags which was
  produced by a FormatIdOutputStream.

  <P>Please see the documentation for FormatIdOutputStream for
  information about the streams format and capabilites.
  */
public final class FormatIdInputStream extends DataInputStream
    implements ErrorObjectInput, Resetable, CloneableStream
{
	protected ClassFactory cf;
	private ErrorInfo errorInfo;
    private Exception myNestedException;


	/**
	  Constructor for a FormatIdInputStream

	  @param in bytes come from here.
	  */
    public FormatIdInputStream(InputStream in)
	{
		super(in);
	}

	/**
	  Read an object from this stream.

	  @return The read object.
	  @exception java.io.IOException An IO or serialization error occured.
	  @exception java.lang.ClassNotFoundException A class for an object in
	  the stream could not be found.
	  */

	public Object readObject() throws IOException, ClassNotFoundException
	{
        setErrorInfo(null);

		int fmtId = FormatIdUtil.readFormatIdInteger(this);

		if (fmtId == StoredFormatIds.NULL_FORMAT_ID)
		{
			return null;
		}

		if (fmtId == StoredFormatIds.STRING_FORMAT_ID)
		{
			return readUTF();
		}

		try
        {

			if (fmtId == StoredFormatIds.SERIALIZABLE_FORMAT_ID)
			{
				ObjectInputStream ois = getObjectStream();
				try {
					return ois.readObject();
				} catch (IOException ioe) {
					setErrorInfo((ErrorInfo) ois);
					throw ioe;
				} catch (ClassNotFoundException cnfe) {
					setErrorInfo((ErrorInfo) ois);
					throw cnfe;
				} catch (LinkageError le) {
					setErrorInfo((ErrorInfo) ois);
					throw le;
				} catch (ClassCastException cce) {
					setErrorInfo((ErrorInfo) ois);
					throw cce;
				}
			}

			try {

				Formatable f = (Formatable)Monitor.newInstanceFromIdentifier(fmtId);
				if (f instanceof Storable)
				{
					boolean isNull = this.readBoolean();
					if (isNull == true)
					{
						Storable s = (Storable)f;
						s.restoreToNull();
						return s;
					}
				}

				f.readExternal(this);
				return f;
			} catch (StandardException se) {
				throw new ClassNotFoundException(se.toString());
			}


		}
        catch (ClassCastException cce)
        {
			// We catch this here as it is usuall a user error.
			// they have readExternal (or SQLData) that doesn't match
			// the writeExternal. and thus the object read is of
			// the incorrect type, e.g. Integer i = (Integer) in.readObject();
			StreamCorruptedException sce = new StreamCorruptedException(cce.toString());
			sce.initCause(cce);
			throw sce;
		}
	}

	/**
	  Set the InputStream for this FormatIdInputStream to the stream
	  provided.

	  @param in The new input stream.
	  */
	public void setInput(InputStream in)
	{
		this.in = in;
	}

    public	InputStream	getInputStream()
    {
        return in;
    }

	public String getErrorInfo()
    {
		if (errorInfo == null)
            return "";

		return errorInfo.getErrorInfo();
	}

    public Exception getNestedException()
    {
        if (myNestedException != null)
            return null;

        if (errorInfo == null)
            return null;

        return errorInfo.getNestedException();
    }

	private void setErrorInfo(ErrorInfo ei)
    {
        errorInfo = ei;
	}


    ClassFactory getClassFactory() {
		if (cf == null) {

			ClassFactoryContext cfc =
				(ClassFactoryContext) ContextService.getContextOrNull
				                                  (ClassFactoryContext.CONTEXT_ID);

			if (cfc != null)
				cf = cfc.getClassFactory();
		}
		return cf;
	}

	/*
	** Class private methods
	*/

	private ObjectInputStream getObjectStream() throws IOException {

		return getClassFactory() == null ?
			new ObjectInputStream(this) :
			new ApplicationObjectInputStream(this, cf);
	}



    /*** Resetable interface ***/

    /** @see Resetable#resetStream() */
    public void resetStream()
        throws IOException, StandardException
    {
        ((Resetable) in).resetStream();
    }


    /** @see Resetable#initStream() */
    public void initStream()
        throws StandardException
    {
        ((Resetable) in).initStream();
    }


    /** @see Resetable#closeStream() */
    public void closeStream()
    {
        ((Resetable) in).closeStream();
    }

    /*** CloneableStream interface ***/

    /** @see CloneableStream#cloneStream() */
    public InputStream cloneStream() {
        InputStream new_input_stream = ((CloneableStream) in).cloneStream();

        return(new FormatIdInputStream(new_input_stream));
    }
}
