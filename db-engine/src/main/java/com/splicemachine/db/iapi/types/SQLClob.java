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
 * All such Splice Machine modifications are Copyright 2012 - 2018 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.jdbc.CharacterStreamDescriptor;

import com.splicemachine.db.iapi.services.io.ArrayInputStream;
import com.splicemachine.db.iapi.services.io.CloneableStream;
import com.splicemachine.db.iapi.services.io.FormatIdInputStream;
import com.splicemachine.db.iapi.services.io.InputStreamUtil;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.util.UTF8Util;

import com.splicemachine.db.shared.common.reference.SQLState;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.PushbackInputStream;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.RuleBasedCollator;
import java.util.Calendar;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl.Format;
import org.spark_project.guava.io.CharStreams;


/**
 * SQLClob represents a CLOB value with UCS_BASIC collation.
 * CLOB supports LIKE operator only for collation.
 */
public class SQLClob
	extends SQLVarchar
{
    /** The header generator used for 10.4 (or older) databases. */
    private static final StreamHeaderGenerator TEN_FOUR_CLOB_HEADER_GENERATOR =
            new ClobStreamHeaderGenerator(true);

    /** The header generator used for 10.5 databases. */
    private static final StreamHeaderGenerator TEN_FIVE_CLOB_HEADER_GENERATOR =
            new ClobStreamHeaderGenerator(false);

    /**
     * The maximum number of bytes used by the stream header.
     * <p>
     * Use the length specified by the ten five header generator.
     */
    private static final int MAX_STREAM_HEADER_LENGTH =
            TEN_FIVE_CLOB_HEADER_GENERATOR.getMaxHeaderLength();

    /**
     * The descriptor for the stream. If there is no stream this should be
     * {@code null}, which is also true if the descriptor hasen't been
     * constructed yet.
     * <em>Note</em>: Always check if {@code stream} is non-null before using
     * the information stored in the descriptor internally.
     */
    private CharacterStreamDescriptor csd;

    /** Tells if the database is being accessed in soft upgrade mode. */
    private Boolean inSoftUpgradeMode = null;

	/*
	 * DataValueDescriptor interface.
	 *
	 * These are actually all implemented in the super-class, but we need
	 * to duplicate some of them here so they can be called by byte-code
	 * generation, which needs to know the class the method appears in.
	 */

	public String getTypeName()
	{
		return TypeId.CLOB_NAME;
	}

	/*
	 * DataValueDescriptor interface
	 */

    /**
     * Returns a clone of this CLOB value.
     * <p>
     * Unlike the other binary types, CLOBs can be very large. We try to clone
     * the underlying stream when possible to avoid having to materialize the
     * value into memory.
     *
     * @param forceMaterialization any streams representing the data value will
     *      be materialized if {@code true}, the data value will be kept as a
     *      stream if possible if {@code false}
     * @return A clone of this CLOB value.
     * @see DataValueDescriptor#cloneValue
     */
    public DataValueDescriptor cloneValue(boolean forceMaterialization) {
        // TODO: Add optimization for materializing "smallish" streams. This
        //       may be more effective because the data doesn't have to be
        //       decoded multiple times.
        final SQLClob clone = new SQLClob();
        // Copy the soft upgrade mode state.
        clone.inSoftUpgradeMode = inSoftUpgradeMode;
        
        // Shortcut cases where the value is NULL.
        if (isNull()) {
            return clone;
        }

        if (!forceMaterialization) {
            if (stream != null && stream instanceof CloneableStream) {
                int length = UNKNOWN_LOGICAL_LENGTH;
                if (csd != null && csd.getCharLength() > 0) {
                    length = (int)csd.getCharLength();
                }
                clone.setValue(((CloneableStream)stream).cloneStream(), length);
            } else if (_clobValue != null) {
                // Assumes the Clob object can be shared between value holders.
                clone.setValue(_clobValue);
            }
            // At this point we may still not have cloned the value because we
            // have a stream that isn't cloneable.
            // TODO: Add functionality to materialize to temporary disk storage
            //       to avoid OOME for large CLOBs.
        }

        // See if we are forced to materialize the value, either because
        // requested by the user or because we don't know how to clone it.
        if (clone.isNull() || forceMaterialization) {
            try {
                clone.setValue(getString());
            } catch (StandardException se) {
                if (SanityManager.DEBUG) {
                    SanityManager.THROWASSERT("Unexpected exception", se);
                }
                return null;
            }
        }
        return clone;
    }

	/**
	 * @see DataValueDescriptor#getNewNull
	 *
	 */
	public DataValueDescriptor getNewNull()
	{
        SQLClob newClob = new SQLClob();
        // Copy the soft upgrade mode state.
        newClob.inSoftUpgradeMode = inSoftUpgradeMode;
        return newClob;
	}

	/** @see StringDataValue#getValue(RuleBasedCollator) */
	public StringDataValue getValue(RuleBasedCollator collatorForComparison)
	{
		if (collatorForComparison == null)
		{//null collatorForComparison means use UCS_BASIC for collation
		    return this;			
		} else {
			//non-null collatorForComparison means use collator sensitive
			//implementation of SQLClob
		     CollatorSQLClob s = new CollatorSQLClob(collatorForComparison);
		     s.copyState(this);
		     return s;
		}
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */

	/**
		Return my format identifier.

		@see com.splicemachine.db.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_CLOB_ID;
	}

	/*
	 * constructors
	 */

	public SQLClob()
	{
	}

	public SQLClob(String val)
	{
		super(val);
	}

	public SQLClob(Clob val)
	{
		super(val);
	}

	/*
	 * DataValueDescriptor interface
	 */

	/* @see DataValueDescriptor#typePrecedence */
	public int typePrecedence()
	{
		return TypeId.CLOB_PRECEDENCE;
	}

	/*
	** disable conversions to/from most types for CLOB.
	** TEMP - real fix is to re-work class hierachy so
	** that CLOB is towards the root, not at the leaf.
	*/

	public boolean	getBoolean() throws StandardException
	{
		throw dataTypeConversion("boolean");
	}

	public byte	getByte() throws StandardException
	{
		throw dataTypeConversion("byte");
	}

	public short	getShort() throws StandardException
	{
		throw dataTypeConversion("short");
	}

	public int	getInt() throws StandardException
	{
        if (isNull())
            return 0;

        try
        {
            return Integer.parseInt(getString().trim());
        }
        catch (NumberFormatException nfe)
        {
            throw StandardException.newException(
                    com.splicemachine.db.iapi.reference.SQLState.LANG_FORMAT_EXCEPTION, "int");
        }
	}

    /**
     * Returns the character length of this Clob.
     * <p>
     * If the value is stored as a stream, the stream header will be read. If
     * the stream header doesn't contain the stream length, the whole stream
     * will be decoded to determine the length.
     *
     * @return The character length of this Clob.
     * @throws StandardException if obtaining the length fails
     */
    public int getLength() throws StandardException {
        if (stream == null) {
            return super.getLength();
        }
        //
        // The following check was put in to fix DERBY-4544. We seem to get
        // confused if we have to re-use non-resetable streams.
        //
        if ( !(stream instanceof Resetable) ) { return super.getLength(); }
        
        // The Clob is represented as a stream.
        // Make sure we have a stream descriptor.
        boolean repositionStream = (csd != null);
        if (csd == null) {
            getStreamWithDescriptor();
            // We know the stream is at the first char position here.
        }
        if (csd.getCharLength() != 0) {
            return (int)csd.getCharLength();
        }
        // We now know that the Clob is represented as a stream, but not if the
        // length is unknown or actually zero. Check.
        if (SanityManager.DEBUG) {
            // The stream isn't expecetd to be position aware here.
            SanityManager.ASSERT(!csd.isPositionAware());
        }
        long charLength = 0;
        try {
            if (repositionStream) {
                rewindStream(stream, csd.getDataOffset());
            }
            charLength = UTF8Util.skipUntilEOF(stream);
            // We just drained the whole stream. Reset it.
            rewindStream(stream, 0);
        } catch (IOException ioe) {
            throwStreamingIOException(ioe);
        }
        // Update the descriptor in two ways;
        //   (1) Set the char length, whether it is zero or not.
        //   (2) Set the current byte pos to zero.
        csd = new CharacterStreamDescriptor.Builder().copyState(csd).
                charLength(charLength).curBytePos(0).
                curCharPos(CharacterStreamDescriptor.BEFORE_FIRST).build();
        return (int)charLength;
    }

	public long	getLong() throws StandardException
	{
        if (isNull())
            return 0;

        try
        {
            return Long.parseLong(getString().trim());

        }
        catch (NumberFormatException nfe)
        {
            throw StandardException.newException(
                    com.splicemachine.db.iapi.reference.SQLState.LANG_FORMAT_EXCEPTION, "long");
        }
	}

	public float	getFloat() throws StandardException
	{
        if (isNull())
            return 0;

        try
        {
            return new Float(getString().trim());
        }
        catch (NumberFormatException nfe)
        {
            throw StandardException.newException(
                    com.splicemachine.db.iapi.reference.SQLState.LANG_FORMAT_EXCEPTION, "float");
        }
	}

	public double	getDouble() throws StandardException
	{
        if (isNull())
            return 0;
        try
        {
            return new Double(getString().trim());
        }
        catch (NumberFormatException nfe)
        {
            throw StandardException.newException(
                    com.splicemachine.db.iapi.reference.SQLState.LANG_FORMAT_EXCEPTION, "double");
        }
	}
	public int typeToBigDecimal() throws StandardException
	{
        return java.sql.Types.CHAR;
	}
	public byte[]	getBytes() throws StandardException
	{
		throw dataTypeConversion("byte[]");
	}

	public Date	getDate(java.util.Calendar cal) throws StandardException
	{
        return getDate(cal, getString(), getLocaleFinder());
	}

    /**
     * @exception StandardException     Thrown on error
     */
    public Object   getObject() throws StandardException
    {
        if ( _clobValue != null ) { return _clobValue; }
        else
        {
            String stringValue = getString();

            if ( stringValue == null ) { return null; }
            else { return new HarmonySerialClob( stringValue.toCharArray() ); }
        }
    }

    /**
     * Returns a descriptor for the input stream for this CLOB value.
     * <p>
     * The descriptor contains information about header data, current positions,
     * length, whether the stream should be buffered or not, and if the stream
     * is capable of repositioning itself.
     * <p>
     * When this method returns, the stream is positioned on the first
     * character position, such that the next read will return the first
     * character in the stream.
     *
     * @return A descriptor for the stream, which includes a reference to the
     *      stream itself. If the value cannot be represented as a stream,
     *      {@code null} is returned instead of a descriptor.
     * @throws StandardException if obtaining the descriptor fails
     */
    public CharacterStreamDescriptor getStreamWithDescriptor()
            throws StandardException {
        if (stream == null) {
            // Lazily reset the descriptor here, to avoid further changes in
            // {@code SQLChar}.
            csd = null;
            throw StandardException.newException(
                    SQLState.LANG_STREAM_INVALID_ACCESS, getTypeName());
        }
        // NOTE: Getting down here several times is potentially dangerous.
        // When the stream is published, we can't assume we know the position
        // any more. The best we can do, which may hurt performance to some
        // degree in some non-recommended use-cases, is to reset the stream if
        // possible.
        if (csd != null) {
            if (stream instanceof Resetable) {
                try {
                    ((Resetable)stream).resetStream();
                    // Make sure the stream is in sync with the descriptor.
                    InputStreamUtil.skipFully(stream, csd.getCurBytePos());
                } catch (IOException ioe) {
                    throwStreamingIOException(ioe);
                }
            } else {
                if (SanityManager.DEBUG) {
                    SanityManager.THROWASSERT("Unable to reset stream when " +
                            "fetched the second time: " + stream.getClass());
                }
            }
        }

        if (csd == null) {
            // First time, read the header format of the stream.
            try {
                // Assume new header format, adjust later if necessary.
                byte[] header = new byte[MAX_STREAM_HEADER_LENGTH];
                int read = stream.read(header);
                // Expect at least two header bytes.
                if (SanityManager.DEBUG) {
                    SanityManager.ASSERT(read > 1,
                            "Too few header bytes: " + read);
                }
                HeaderInfo hdrInfo = investigateHeader(header, read);
                if (read > hdrInfo.headerLength()) {
                    // We have read too much. Reset the stream.
                    read = hdrInfo.headerLength();
                    rewindStream(stream, read);
                }
                csd = new CharacterStreamDescriptor.Builder().stream(stream).
                    bufferable(false).positionAware(false).
                    curCharPos(read == 0 ?
                        CharacterStreamDescriptor.BEFORE_FIRST : 1).
                    curBytePos(read).
                    dataOffset(hdrInfo.headerLength()).
                    byteLength(hdrInfo.byteLength()).
                    charLength(hdrInfo.charLength()).build();
            } catch (IOException ioe) {
                // Check here to see if the root cause is a container closed
                // exception. If so, this most likely means that the Clob was
                // accessed after a commit or rollback on the connection.
                Throwable rootCause = ioe;
                while (rootCause.getCause() != null) {
                    rootCause = rootCause.getCause();
                }
                if (rootCause instanceof StandardException) {
                    StandardException se = (StandardException)rootCause;
                    if (se.getMessageId().equals(
                            SQLState.DATA_CONTAINER_CLOSED)) {
                        throw StandardException.newException(
                                SQLState.BLOB_ACCESSED_AFTER_COMMIT, ioe);
                    }
                }
                throwStreamingIOException(ioe);
            }
        }
        return this.csd;
    }

    /**
     * Tells if this CLOB value is, or will be, represented by a stream.
     *
     * @return {@code true} if the value is represented by a stream,
     *      {@code false} otherwise.
     */
    public boolean hasStream() {
        return stream != null;
    }

	public Time	getTime(java.util.Calendar cal) throws StandardException
	{
		throw dataTypeConversion("java.sql.Time");
	}

	public Timestamp	getTimestamp(java.util.Calendar cal) throws StandardException
	{
		throw dataTypeConversion("java.sql.Timestamp");
	}
    
    /**
     * Gets a trace representation of the CLOB for debugging.
     *
     * @return a trace representation of the CLOB.
     */
    public final String getTraceString() throws StandardException {
        // Check if the value is SQL NULL.
        if (isNull()) {
            return "NULL";
        }

        // Check if we have a stream.
        if (hasStream()) {
            return (getTypeName() + "(" + getStream().toString() + ")");
        }

        return (getTypeName() + "(" + getLength() + ")");
    }
    
    /**
     * Normalization method - this method may be called when putting
     * a value into a SQLClob, for example, when inserting into a SQLClob
     * column.  See NormalizeResultSet in execution.
     * Per the SQL standard ,if the clob column is not big enough to 
     * hold the value being inserted,truncation error will result
     * if there are trailing non-blanks. Truncation of trailing blanks
     * is allowed.
     * @param desiredType   The type to normalize the source column to
     * @param sourceValue   The value to normalize
     *
     *
     * @exception StandardException             Thrown for null into
     *                                          non-nullable column, and for
     *                                          truncation error
     */

    public void normalize(
                DataTypeDescriptor desiredType,
                DataValueDescriptor sourceValue)
                    throws StandardException
    {
        // if sourceValue is of type clob, and has a stream,
        // dont materialize it here (as the goal of using a stream is to
        // not have to materialize whole object in memory in the server), 
        // but instead truncation checks will be done when data is streamed in.
        // (see ReaderToUTF8Stream) 
        // if sourceValue is not a stream, then follow the same
        // protocol as varchar type for normalization
        if( sourceValue instanceof SQLClob)
        {
            SQLClob clob = (SQLClob)sourceValue;
            if (clob.stream != null)
            {
                copyState(clob);
                return;
            }
        }
        
        super.normalize(desiredType,sourceValue);
    }

	public void setValue(Time theValue, Calendar cal) throws StandardException
	{
		throwLangSetMismatch("java.sql.Time");
	}
	
	public void setValue(Timestamp theValue, Calendar cal) throws StandardException
	{
		throwLangSetMismatch("java.sql.Timestamp");
	}
	
	public void setValue(Date theValue, Calendar cal) throws StandardException
	{
		throwLangSetMismatch("java.sql.Date");
	}
	
	public void setBigDecimal(Number bigDecimal) throws StandardException
	{
		throwLangSetMismatch("java.math.BigDecimal");
	}

    /**
     * Sets a new stream for this CLOB.
     *
     * @param stream the new stream
     */
    public final void setStream(InputStream stream) {
        super.setStream(stream);
        // Discard the old stream descriptor.
        this.csd = null;
    }

    public final void restoreToNull() {
        this.csd = null;
        super.restoreToNull();
    }

	public void setValue(int theValue) throws StandardException
	{
		throwLangSetMismatch("int");
	}

	public void setValue(double theValue) throws StandardException
	{
		throwLangSetMismatch("double");
	}

	public void setValue(float theValue) throws StandardException
	{
		throwLangSetMismatch("float");
	}
 
	public void setValue(short theValue) throws StandardException
	{
		throwLangSetMismatch("short");
	}

	public void setValue(long theValue) throws StandardException
	{
		throwLangSetMismatch("long");
	}


	public void setValue(byte theValue) throws StandardException
	{
		throwLangSetMismatch("byte");
	}

	public void setValue(boolean theValue) throws StandardException
	{
		throwLangSetMismatch("boolean");
	}

	public void setValue(byte[] theValue) throws StandardException
	{
		throwLangSetMismatch("byte[]");
	}
    
    /**
     * Set the value from an non-null Java.sql.Clob object.
     */
    final void setObject(Object theValue)
        throws StandardException
    {
        Clob vc = (Clob) theValue;
        
        try {
            long vcl = vc.length();
            if (vcl < 0L || vcl > Integer.MAX_VALUE)
                throw this.outOfRange();
            // For small values, just materialize the value.
            // NOTE: Using streams for the empty string ("") isn't supported
            // down this code path when in soft upgrade mode, because the code
            // reading the header bytes ends up reading zero bytes (i.e., it
            // doesn't get the header / EOF marker).
            if (vcl < 32*1024) {
                setValue(vc.getSubString(1, (int)vcl));
            } else {
                ReaderToUTF8Stream utfIn = new ReaderToUTF8Stream(
                        vc.getCharacterStream(), (int) vcl, 0, TypeId.CLOB_NAME,
                        getStreamHeaderGenerator());
                setValue(utfIn, (int) vcl);
            }
        } catch (SQLException e) {
            throw dataTypeConversion("DAN-438-tmp");
       }
    }

    /**
     * Writes the CLOB data value to the given destination stream using the
     * modified UTF-8 format.
     *
     * @param out destination stream
     * @throws IOException if writing to the destination stream fails
     */
    public void writeExternal(ObjectOutput out)
            throws IOException {
        out.writeBoolean(isNull());
        if (!isNull) {
            super.writeClobUTF(out);
        }
    }

    /**
     * Returns a stream header generator for a Clob.
     * <p>
     * <em>NOTE</em>: To guarantee a successful generation, one of the following
     * two conditions must be met at header or EOF generation time:
     * <ul> <li>{@code setStreamHeaderFormat} has been invoked before the header
     *          generator was obtained.</li>
     *      <li>There is context at generation time, such that the mode can be
     *          determined by obtaining the database context and by consulting
     *          the data dictionary.</li>
     * </ul>
     *
     * @return A stream header generator.
     */
    public StreamHeaderGenerator getStreamHeaderGenerator() {
        if (inSoftUpgradeMode == null) {
            // We don't know which mode we are running in, return a generator
            // the will check this when asked to generate the header.
            return new ClobStreamHeaderGenerator(this);
        } else {
            if (inSoftUpgradeMode == Boolean.TRUE) {
                return TEN_FOUR_CLOB_HEADER_GENERATOR;
            } else {
                return TEN_FIVE_CLOB_HEADER_GENERATOR;
            }
        }
    }

    /**
     * Tells whether the database is being accessed in soft upgrade mode or not.
     *
     * @param inSoftUpgradeMode {@code TRUE} if the database is accessed in
     *      soft upgrade mode, {@code FALSE} is not, or {@code null} if unknown
     */
    public void setStreamHeaderFormat(Boolean inSoftUpgradeMode) {
        this.inSoftUpgradeMode = inSoftUpgradeMode;
    }

    /**
     * Investigates the header and returns length information.
     *
     * @param hdr the raw header bytes
     * @param bytesRead number of bytes written into the raw header bytes array
     * @return The information obtained from the header.
     * @throws IOException if the header format is invalid, or the stream
     *      seems to have been corrupted
     */
    private HeaderInfo investigateHeader(byte[] hdr, int bytesRead)
            throws IOException {
        int dataOffset = MAX_STREAM_HEADER_LENGTH;
        int utfLen = -1;
        int strLen = -1;

        // Peek at the magic byte.
        if (bytesRead < dataOffset || (hdr[2] & 0xF0) != 0xF0) {
            // We either have a very short value with the old header
            // format, or the stream is corrupted.
            // Assume the former and check later (see further down).
            dataOffset = 2;
        }

        // Do we have a pre 10.5 header?
        if (dataOffset == 2) {
            // Note that we add the two bytes holding the header to the total
            // length only if we know how long the user data is.
            utfLen = ((hdr[0] & 0xFF) << 8) | ((hdr[1] & 0xFF));
            // Sanity check for small streams:
            // The header length pluss the encoded length must be equal to the
            // number of bytes read.
            if (bytesRead < MAX_STREAM_HEADER_LENGTH) {
                if (dataOffset + utfLen != bytesRead) {
                    throw new IOException("Corrupted stream; headerLength=" +
                            dataOffset + ", utfLen=" + utfLen + ", bytesRead=" +
                            bytesRead);
                }
            }
            if (utfLen > 0) {
                utfLen += dataOffset;
            }
        } else if (dataOffset == 5) {
            // We are dealing with the 10.5 stream header format.
            int hdrFormat = hdr[2] & 0x0F;
            switch (hdrFormat) {
                case 0: // 0xF0
                    strLen = (
                                ((hdr[0] & 0xFF) << 24) |
                                ((hdr[1] & 0xFF) << 16) |
                                // Ignore the third byte (index 2).
                                ((hdr[3] & 0xFF) <<  8) |
                                ((hdr[4] & 0xFF) <<  0)
                             );
                    break;
                default:
                    // We don't know how to handle this header format.
                    throw new IOException("Invalid header format " +
                            "identifier: " + hdrFormat + "(magic byte is 0x" +
                            Integer.toHexString(hdr[2] & 0xFF) + ")");
            }
        }
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(utfLen > -1 || strLen > -1);
        }
        return new HeaderInfo(dataOffset, dataOffset == 5 ? strLen : utfLen);
    }

    /**
     * Reads and materializes the CLOB value from the stream.
     *
     * @param in source stream
     * @throws java.io.UTFDataFormatException if an encoding error is detected
     * @throws IOException if reading from the stream fails, or the content of
     *      the stream header is invalid
     */
    public void readExternal(ObjectInput in)
            throws IOException {
        if (in.readBoolean()) {
            setIsNull(true);
            return;
        }
        HeaderInfo hdrInfo;
        if (csd != null) {
            int hdrLen = (int)csd.getDataOffset();
            int valueLength = (hdrLen == 5) ? (int)csd.getCharLength()
                                            : (int)csd.getByteLength();
            hdrInfo = new HeaderInfo(hdrLen, valueLength);
            // Make sure the stream is correctly positioned.
            rewindStream((InputStream)in, hdrLen);
        } else {
            final InputStream srcIn = (InputStream)in;
            final boolean markSet = srcIn.markSupported();
            if (markSet) {
                srcIn.mark(MAX_STREAM_HEADER_LENGTH);
            }
            byte[] header = new byte[MAX_STREAM_HEADER_LENGTH];
            int read = in.read(header);
            // Try to read MAX_STREAM_HEADER_LENGTH bytes
            while (read < MAX_STREAM_HEADER_LENGTH) {
                int r = in.read(header, read, MAX_STREAM_HEADER_LENGTH - read);
                if (r == -1) {
                    // EOF, break loop with what we've already read
                    break;
                }
                read += r;
            }
            // Expect at least two header bytes.
            if (SanityManager.DEBUG) {
                SanityManager.ASSERT(read > 1, "Too few header bytes: " + read);
            }
            hdrInfo = investigateHeader(header, read);
            if (read > hdrInfo.headerLength()) {
                // We read too much data. To "unread" the bytes, the following
                // mechanisms will be attempted:
                //  1) See if we set a mark on the stream, if so reset it.
                //  2) If we have a FormatIdInputStream, use a
                //     PushBackInputStream and use it as the source.
                //  3) Try using the Resetable interface.
                // To avoid silent data truncation / data corruption, we fail
                // in step three if the stream isn't resetable.
                if (markSet) {
                    // 1) Reset the stream to the previously set mark.
                    srcIn.reset();
                    InputStreamUtil.skipFully(srcIn, hdrInfo.headerLength());
                } else if (in instanceof FormatIdInputStream) {
                    // 2) Add a push back stream on top of the underlying
                    // source, and unread the surplus bytes we read. Set the
                    // push back stream to be the source of the data input obj.
                    final int surplus = read - hdrInfo.headerLength();
                    FormatIdInputStream formatIn = (FormatIdInputStream)in;
                    PushbackInputStream pushbackIn = new PushbackInputStream(
                            formatIn.getInputStream(), surplus);
                    pushbackIn.unread(header, hdrInfo.headerLength(), surplus);
                    formatIn.setInput(pushbackIn);
                } else {
                    // 3) Assume we have a store stream.
                    rewindStream(srcIn, hdrInfo.headerLength());
                }
            }
        }
        // The data will be materialized in memory, in a char array.
        // Subtract the header length from the byte length if there is a byte
        // encoded in the header, otherwise the decode routine will try to read
        // too many bytes.
        int byteLength = 0; // zero is interpreted as unknown / unset
        if (hdrInfo.byteLength() != 0) {
            byteLength = hdrInfo.byteLength() - hdrInfo.headerLength();
        }
        super.readExternal(in, byteLength, hdrInfo.charLength());
    }

    /**
     * Reads and materializes the CLOB value from the stream.
     *
     * @param in source stream
     * @throws java.io.UTFDataFormatException if an encoding error is detected
     * @throws IOException if reading from the stream fails, or the content of
     *      the stream header is invalid
     */
    public void readExternalFromArray(ArrayInputStream in)
            throws IOException {
        // It is expected that the position of the array input stream has been
        // set to the correct position before this method is invoked.
        int prevPos = in.getPosition();
        byte[] header = new byte[MAX_STREAM_HEADER_LENGTH];
        int read = in.read(header);
        // Expect at least two header bytes.
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(read > 1, "Too few header bytes: " + read);
        }
        HeaderInfo hdrInfo = investigateHeader(header, read);
        if (read > hdrInfo.headerLength()) {
            // Reset stream. This path will only be taken for Clobs stored
            // with the pre 10.5 stream header format.
            // Note that we set the position to before the header again, since
            // we know the header will be read again.
            in.setPosition(prevPos);
            super.readExternalFromArray(in);
        } else {
            // We read only header bytes, next byte is user data.
            super.readExternalClobFromArray(in, hdrInfo.charLength());
        }
    }

    /**
     * Rewinds the stream to the beginning and then skips the specified number
     * of bytes.
     *
     * @param in input stream to rewind
     * @param offset number of bytes to skip
     * @throws IOException if resetting or reading from the stream fails
     */
    private void rewindStream(InputStream in, long offset)
            throws IOException {
        try {
            ((Resetable)in).resetStream();
            InputStreamUtil.skipFully(in, offset);
        } catch (StandardException se) {
            IOException ioe = new IOException(se.getMessage());
            ioe.initCause(se);
            throw ioe;
        }
    }

    /**
     * Holder class for header information gathered from the raw byte header in 
     * the stream.
     */
    //@Immutable
    private static class HeaderInfo {

        /** The value length, either in bytes or characters. */
        private final int valueLength;
        /** The header length in bytes. */
        private final int headerLength;

        /**
         * Creates a new header info object.
         *
         * @param headerLength the header length in bytes
         * @param valueLength the value length (chars or bytes)
         */
        HeaderInfo(int headerLength, int valueLength) {
            this.headerLength = headerLength;
            this.valueLength = valueLength;
        }

        /**
         * Returns the header length in bytes.
         *
         * @return Number of bytes occupied by the header.
         */
       int headerLength() {
           return this.headerLength;
       }

       /**
        * Returns the character length encoded in the header, if any.
        *
        * @return A positive integer if a character count was encoded in the
        *       header, or {@code 0} (zero) if the header contained byte length
        *       information.
        */
       int charLength() {
           return isCharLength() ? valueLength : 0;
       }

       /**
        * Returns the byte length encoded in the header, if any.
        *
        * @return A positive integer if a byte count was encoded in the
        *       header, or {@code 0} (zero) if the header contained character
        *       length information.
        */
       int byteLength() {
           return isCharLength() ? 0 : valueLength;
       }

       /**
        * Tells whether the encoded length was in characters or bytes.
        *
        * @return {@code true} if the header contained a character count,
        *       {@code false} if it contained a byte count.
        */
       boolean isCharLength() {
           return (headerLength == 5);
       }

       /**
        * Returns a textual representation.
        */
       public String toString() {
           return ("headerLength=" + headerLength + ", valueLength= " +
                   valueLength + ", isCharLength=" + isCharLength());
       }
    }
    public Format getFormat() {
    	return Format.CLOB;
    }

    @Override
    public Object getSparkObject() throws StandardException {
        try {
            Clob clob = (Clob) getObject();
            return clob == null ? null : CharStreams.toString(clob.getCharacterStream());
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }
}
