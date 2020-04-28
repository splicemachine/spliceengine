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

package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.db.DatabaseContext;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.jdbc.CharacterStreamDescriptor;
import com.splicemachine.db.iapi.reference.ContextId;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.cache.ClassSize;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.i18n.LocaleFinder;
import com.splicemachine.db.iapi.services.io.*;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.StatementContext;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl.Format;
import com.splicemachine.db.iapi.util.StringUtil;
import com.splicemachine.db.iapi.util.UTF8Util;
import com.yahoo.sketches.theta.UpdateSketch;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.joda.time.DateTime;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.text.CollationKey;
import java.text.RuleBasedCollator;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Locale;
import java.util.MissingResourceException;


/**

 The SQLChar represents a CHAR value with UCS_BASIC collation.
 SQLChar may be used directly by any code when it is guaranteed
 that the required collation is UCS_BASIC, e.g. system columns.
 <p>
 The state may be in char[], a String, a Clob, or an unread stream, depending
 on how the datatype was created.
 <p>
 Stream notes:
 <p>
 When the datatype comes from the database layer and the length of the bytes
 necessary to store the datatype on disk exceeds the size of a page of the
 container holding the data then the store returns a stream rather than reading
 all the bytes into a char[] or String.  The hope is that the usual usage case
 is that data never need be expanded in the db layer, and that client can
 just be given a stream that can be read a char at a time through the jdbc
 layer.  Even though SQLchar's can't ever be this big, this code is shared
 by all the various character datatypes including SQLClob which is expected
 to usually larger than a page.
 <p>
 The state can also be a stream in the case of insert/update where the client
 has used a jdbc interface to set the value as a stream rather than char[].
 In this case the hope is that the usual usage case is that stream never need
 be read until it is passed to store, read once, and inserted into the database.

 **/

public class SQLChar
    extends DataType implements StringDataValue, StreamStorable
{
    private static Logger logger = Logger.getLogger(SQLChar.class);
    /**************************************************************************
     * static fields of the class
     **************************************************************************
     */


    static final long serialVersionUID = 1234548923L;


    /**
     * The pad character (space).
     */
    public static final char PAD = '\u0020';

    /**
     * threshold, that decides when we return space back to the VM
     * see getString() where it is used
     */
    protected final static int RETURN_SPACE_THRESHOLD = 4096;

    /**
     * when we know that the array needs to grow by at least
     * one byte, it is not performant to grow by just one byte
     * instead this amount is used to provide a reasonable growby size.
     */
    private final static int GROWBY_FOR_CHAR = 64;


    private static final int BASE_MEMORY_USAGE =
        ClassSize.estimateBaseFromCatalog( SQLChar.class);

    /**
        Static array that can be used for blank padding.
    */
    private static final char[] BLANKS = new char[40];
    static {
        for (int i = 0; i < BLANKS.length; i++) {
            BLANKS[i] = ' ';
        }
    }

    /**
     * Stream header generator for CHAR, VARCHAR and LONG VARCHAR. Currently,
     * only one header format is used for these data types.
     */
    protected static final StreamHeaderGenerator CHAR_HEADER_GENERATOR =
            new CharStreamHeaderGenerator();

    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */

    /*
     * object state
     */

    // Don't use value directly in most situations. Use getString()
    // OR use the rawData array if rawLength != -1.
    private     String  value;

    // rawData holds the reusable array for reading in SQLChars. It contains a
    // valid value if rawLength is greater than or equal to 0. See getString()
    // to see how it is converted to a String. Even when converted to a String
    // object the rawData array remains for potential future use, unless
    // rawLength is > 4096. In this case the rawData is set to null to avoid
    // huge memory use.
    private     char[]  rawData;
    private     int     rawLength = -1;

    // This field stores the defined size of SQL CHAR(.), to give OLAP Spark the needed information.
    // (Since Spark SQL does not support fixed-length string.)
    // To maintain backward compatibility, this field is not ser/de.
    // As long as OLAP invokes ActivationHolder.reinitialize(), this field is reinitialized properly.
    private     int     sqlCharSize = -1;

    // For null strings, cKey = null.
    private CollationKey cKey;

    /**
     * The value as a user-created Clob
     */
    protected Clob _clobValue;

    /**
     * The value as a stream in the on-disk format.
     */
    InputStream stream;

    /* Locale info (for International support) */
    private LocaleFinder localeFinder;


    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

    /**
     * no-arg constructor, required by Formattable.
     **/
    public SQLChar()
    {
    }

    public SQLChar(String val)
    {
        setValue (val);
    }

    public SQLChar(Clob val)
    {
        setValue( val );
    }

    /**
     * <p>
     * This is a special constructor used when we need to represent a password
     * as a VARCHAR (see DERBY-866). If you need a general-purpose constructor
     * for char[] values and you want to re-use this constructor, make sure to
     * keep track of the following:
     * </p>
     *
     * <ul>
     * <li>A password should never be turned into a String. This is because Java
     * garbage collection makes it easy to sniff memory for String passwords. For
     * more information, see
     * <a href="http://securesoftware.blogspot.com/2009/01/java-security-why-not-to-use-string.html">this blog</a>.</li>
     * <li>It must be possible to 0 out the char[] array wrapped inside this SQLChar. This
     * reduces the vulnerability that someone could sniff the char[] password after Derby
     * has processed it.</li>
     * </ul>
     */
    public SQLChar( char[] val )
    {
        if ( val == null )
        {
            restoreToNull();
        }
        else
        {
            int length = val.length;
            char[]  localCopy = new char[ length ];
            System.arraycopy( val, 0, localCopy, 0, length );

            copyState
                (
                 null,
                 localCopy,
                 length,
                 null,
                 null,
                 null,
                 null
                 );
        }
    }

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    private static void appendBlanks(char[] ca, int offset, int howMany)
    {
        while (howMany > 0)
        {
            int count = howMany > BLANKS.length ? BLANKS.length : howMany;

            System.arraycopy(BLANKS, 0, ca, offset, count);
            howMany -= count;
            offset += count;
        }
    }

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    /**
     * <p>
     * This is a special accessor used when we wrap passwords in VARCHARs.
     * This accessor copies the wrapped char[] and then fills it with 0s so that
     * the password can't be memory-sniffed. For more information, see the comment
     * on the SQLChar( char[] ) constructor.
     * </p>
     */
    public  char[]  getRawDataAndZeroIt()
    {
        if ( rawData == null ) {
            if (value != null) {
                return value.toCharArray(); // Needs to be hidden
            }

            return null; }

        int length = rawData.length;
        char[] retval = new char[ length ];
        System.arraycopy( rawData, 0, retval, 0, length );

        zeroRawData();

        return retval;
    }

    /**
     * <p>
     * Zero out the wrapped char[] so that it can't be memory-sniffed.
     * This helps us protect passwords. See
     * the comment on the SQLChar( char[] ) constructor.
     * </p>
     */
    public  void  zeroRawData()
    {
        if ( rawData == null ) { return; }

        Arrays.fill( rawData, (char) 0 );
        isNull = evaluateNull();
    }

    /**************************************************************************
     * Public Methods of DataValueDescriptor interface:
     *     Mostly implemented in Datatype.
     **************************************************************************
     */

    /**
     * Get Boolean from a SQLChar.
     *
     * <p>
     * Return false for only "0" or "false" for false. No case insensitivity.
     * Everything else is true.
     * <p>
     * The above matches JCC and the client driver.
     *
     *
     * @see DataValueDescriptor#getBoolean
     *
     * @exception StandardException     Thrown on error
     **/
    public boolean getBoolean()
        throws StandardException
    {
        if (isNull())
            return false;

        // Match JCC and the client driver. Match only "0" or "false" for
        // false. No case insensitivity. Everything else is true.

        String cleanedValue = getString().trim();

        return !(cleanedValue.equals("0") || cleanedValue.equals("false"));
    }

    /**
     * Get Byte from a SQLChar.
     *
     * <p>
     * Uses java standard Byte.parseByte() to perform coercion.
     *
     * @see DataValueDescriptor#getByte
     *
     * @exception StandardException thrown on failure to convert
     **/
    public byte getByte() throws StandardException
    {
        if (isNull())
            return (byte)0;

        try
        {
            return Byte.parseByte(getString().trim());
        }
        catch (NumberFormatException nfe)
        {
            throw StandardException.newException(
                    SQLState.LANG_FORMAT_EXCEPTION, "byte");
        }
    }

    @Override
    public byte[] getBytes() throws StandardException {
        if (isNull())
            return null;
        String string = getString();
        if (string != null)
            return string.getBytes(StandardCharsets.UTF_8);
        return null;
    }

    /**
     * Get Short from a SQLChar.
     *
     * <p>
     * Uses java standard Short.parseShort() to perform coercion.
     *
     * @see DataValueDescriptor#getShort
     *
     * @exception StandardException thrown on failure to convert
     **/
    public short getShort() throws StandardException
    {
        if (isNull())
            return (short)0;

        try
        {
            return Short.parseShort(getString().trim());

        }
        catch (NumberFormatException nfe)
        {
            throw StandardException.newException(
                    SQLState.LANG_FORMAT_EXCEPTION, "short");
        }
    }

    /**
     * Get int from a SQLChar.
     *
     * <p>
     * Uses java standard Short.parseInt() to perform coercion.
     *
     * @see DataValueDescriptor#getInt
     *
     * @exception StandardException thrown on failure to convert
     **/
    public int  getInt() throws StandardException
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
                    SQLState.LANG_FORMAT_EXCEPTION, "int");
        }
    }

    /**
     * Get long from a SQLChar.
     *
     * <p>
     * Uses java standard Short.parseLong() to perform coercion.
     *
     * @see DataValueDescriptor#getLong
     *
     * @exception StandardException thrown on failure to convert
     **/
    public long getLong() throws StandardException
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
                    SQLState.LANG_FORMAT_EXCEPTION, "long");
        }
    }

    /**
     * Get float from a SQLChar.
     *
     * <p>
     * Uses java standard Float.floatValue() to perform coercion.
     *
     * @see DataValueDescriptor#getFloat
     *
     * @exception StandardException thrown on failure to convert
     **/
    public float getFloat() throws StandardException
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
                    SQLState.LANG_FORMAT_EXCEPTION, "float");
        }
    }

    /**
     * Get double from a SQLChar.
     *
     * <p>
     * Uses java standard Double.doubleValue() to perform coercion.
     *
     * @see DataValueDescriptor#getDouble
     *
     * @exception StandardException thrown on failure to convert
     **/
    public double getDouble() throws StandardException
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
                    SQLState.LANG_FORMAT_EXCEPTION, "double");
        }
    }

    /**
     * Get date from a SQLChar.
     *
     * @see DataValueDescriptor#getDate
     *
     * @exception StandardException thrown on failure to convert
     **/
    public Date getDate(Calendar cal)
        throws StandardException
    {
        return getDate(cal, getString(), getLocaleFinder());
    }

    /**
     * Get date from a SQLChar.
     *
     * @see DataValueDescriptor#getDate
     *
     * @exception StandardException thrown on failure to convert
     **/
    public DateTime getDateTime() throws StandardException {
        return getDate(getString(), getLocaleFinder());
    }


    public static DateTime getDate(
    String              str,
    LocaleFinder        localeFinder)
        throws StandardException
    {
        if( str == null)
            return null;

        SQLDate internalDate = new SQLDate(str, false, localeFinder);

        return internalDate.getDateTime();
    }

    /**
     * Static function to Get date from a string.
     *
     * @see DataValueDescriptor#getDate
     *
     * @exception StandardException thrown on failure to convert
     **/
    public static Date getDate(
    java.util.Calendar  cal,
    String              str,
    LocaleFinder        localeFinder)
        throws StandardException
    {
        if( str == null)
            return null;

        SQLDate internalDate = new SQLDate(str, false, localeFinder);

        return internalDate.getDate(cal);
    }

    /**
     * Get time from a SQLChar.
     *
     * @see DataValueDescriptor#getTime
     *
     * @exception StandardException thrown on failure to convert
     **/
    public Time getTime(Calendar cal) throws StandardException
    {
        return getTime( cal, getString(), getLocaleFinder());
    }

    /**
     * Static function to Get Time from a string.
     *
     * @see DataValueDescriptor#getTime
     *
     * @exception StandardException thrown on failure to convert
     **/
    public static Time getTime(
    Calendar        cal,
    String          str,
    LocaleFinder    localeFinder)
        throws StandardException
    {
        if( str == null)
            return null;
        SQLTime internalTime = new SQLTime( str, false, localeFinder, cal);
        return internalTime.getTime( cal);
    }

    /**
     * Get Timestamp from a SQLChar.
     *
     * @see DataValueDescriptor#getTimestamp
     *
     * @exception StandardException thrown on failure to convert
     **/
    public Timestamp getTimestamp( Calendar cal) throws StandardException
    {
        return getTimestamp( cal, getString(), getLocaleFinder());
    }

    /**
     * Static function to Get Timestamp from a string.
     *
     * @see DataValueDescriptor#getTimestamp
     *
     * @exception StandardException thrown on failure to convert
     **/
    public static Timestamp getTimestamp(
    java.util.Calendar  cal,
    String              str,
    LocaleFinder        localeFinder)
        throws StandardException
    {
        if( str == null)
            return null;

        SQLTimestamp internalTimestamp =
            new SQLTimestamp( str, false, localeFinder, cal);

        return internalTimestamp.getTimestamp( cal);
    }

    /**************************************************************************
     * Public Methods of StreamStorable interface:
     **************************************************************************
     */
    public InputStream returnStream()
    {
        return stream;
    }

    /**
     * Set this value to the on-disk format stream.
     */
    public void setStream(InputStream newStream) {
        this.value = null;
        this.rawLength = -1;
        this.stream = newStream;
        cKey = null;
        _clobValue = null;
        isNull = evaluateNull();
    }

    public void loadStream() throws StandardException
    {
        getString();
    }


    /**
     * @exception StandardException     Thrown on error
     */
    public Object   getObject() throws StandardException
    {
        return getString();
    }

    /**
     * @exception StandardException     Thrown on error
     */
    public InputStream  getStream() throws StandardException {
        if (!hasStream()) {
            throw StandardException.newException(
                    SQLState.LANG_STREAM_INVALID_ACCESS, getTypeName());
        }
        return stream;
    }

    /**
     * Returns a descriptor for the input stream for this character data value.
     *
     * @return Nothing, throws exception.
     * @throws StandardException if the value isn't represented by a stream
     * @see SQLClob#getStreamWithDescriptor()
     */
    public CharacterStreamDescriptor getStreamWithDescriptor()
            throws StandardException {
        throw StandardException.newException(
                SQLState.LANG_STREAM_INVALID_ACCESS, getTypeName());
    }

    /**
     * CHAR/VARCHAR/LONG VARCHAR implementation.
     * Convert to a BigDecimal using getString.
     */
    public int typeToBigDecimal()  throws StandardException
    {
        return java.sql.Types.CHAR;
    }

    /**
     * @exception StandardException     Thrown on error
     */
    public int getLength() throws StandardException {
        if ( _clobValue != null ) { return getClobLength(); }
        if (rawLength != -1)
            return rawLength;
        if (stream != null) {
            if (stream instanceof Resetable && stream instanceof ObjectInput) {
                try {
                    // Skip the encoded byte length.
                    InputStreamUtil.skipFully(stream, 2);
                    // Decode the whole stream to find the character length.
                    return (int)UTF8Util.skipUntilEOF(stream);
                } catch (IOException ioe) {
                    throwStreamingIOException(ioe);
                } finally {
                    try {
                        ((Resetable) stream).resetStream();
                    } catch (IOException ioe) {
                        throwStreamingIOException(ioe);
                    }
                }
            }
        }
        String tmpString = getString();
        if (tmpString == null) {
            return 0;
        } else {
            return tmpString.length();
        }
    }

    /**
     * Wraps an {@code IOException} in a {@code StandardException} then throws
     * the wrapping exception.
     *
     * @param ioe the {@code IOException} to wrap
     * @throws StandardException the wrapping exception
     */
    protected void throwStreamingIOException(IOException ioe)
            throws StandardException {
        throw StandardException.
            newException(SQLState.LANG_STREAMING_COLUMN_I_O_EXCEPTION,
                         ioe, getTypeName());
    }

    public String getTypeName()
    {
        return TypeId.CHAR_NAME;
    }

    /**
     * If possible, use getCharArray() if you don't really
     * need a string.  getString() will cause an extra
     * char array to be allocated when it calls the the String()
     * constructor (the first time through), so may be
     * cheaper to use getCharArray().
     *
     * @exception StandardException     Thrown on error
     */
    public String getString() throws StandardException
    {
        if (value == null) {

            int len = rawLength;

            if (len != -1) {

                // data is stored in the char[] array

                value = new String(rawData, 0, len);
                if (len > RETURN_SPACE_THRESHOLD) {
                    // free up this char[] array to reduce memory usage
                    rawData = null;
                    rawLength = -1;
                    cKey = null;
                }

            } else if (_clobValue != null) {

                try {
                    value = _clobValue.getSubString( 1L, getClobLength() );
                    _clobValue = null;
                }
                catch (SQLException se) { throw StandardException.plainWrapException( se ); }

            } else if (stream != null) {

                // data stored as a stream
                try {

                    if (stream instanceof FormatIdInputStream) {
                        readExternal((FormatIdInputStream) stream);
                    } else {
                        readExternal(new FormatIdInputStream(stream));
                    }
                    stream = null;

                    // at this point the value is only in the char[]
                    // so call again to convert to a String
                    return getString();

                } catch (IOException ioe) {

                    throw StandardException.newException(
                            SQLState.LANG_STREAMING_COLUMN_I_O_EXCEPTION,
                            ioe,
                            String.class.getName());
                }
            }
            isNull = evaluateNull();
        }

        return value;
    }

    /**
     * Get a char array.  Typically, this is a simple
     * getter that is cheaper than getString() because
     * we always need to create a char array when
     * doing I/O.  Use this instead of getString() where
     * reasonable.
     * <p>
     * <b>WARNING</b>: may return a character array that has spare
     * characters at the end.  MUST be used in conjunction
     * with getLength() to be safe.
     *
     * @exception StandardException     Thrown on error
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "DB-9406")
    public char[] getCharArray() throws StandardException
    {
        if (isNull())
        {
            return null;
        }
        else if (rawLength != -1)
        {
            return rawData;
        }
        else
        {
            // this is expensive -- we are getting a
            // copy of the char array that the
            // String wrapper uses.
            getString();
            char[] data = value.toCharArray();
            setRaw(data, data.length);
            cKey = null;
            return rawData;
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
        return StoredFormatIds.SQL_CHAR_ID;
    }

    /**
     * see if the String value is null.
     * @see Storable#isNull
     */
    private boolean evaluateNull()
    {
        return ((value == null) && (rawLength == -1) && (stream == null) && (_clobValue == null));
    }

    /**
        Writes a non-Clob data value to the modified UTF-8 format used by Derby.

        The maximum stored size is based upon the UTF format
        used to stored the String. The format consists of
        a two byte length field and a maximum number of three
        bytes for each character.
        <BR>
        This puts an upper limit on the length of a stored
        String. The maximum stored length is 65535, these leads to
        the worse case of a maximum string length of 21844 ((65535 - 2) / 3).
        <BR>
        Strings with stored length longer than 64K is handled with
        the following format:
        (1) 2 byte length: will be assigned 0.
        (2) UTF formated string data.
        (3) terminate the string with the following 3 bytes:
            first byte is:
            +---+---+---+---+---+---+---+---+
            | 1 | 1 | 1 | 0 | 0 | 0 | 0 | 0 |
            +---+---+---+---+---+---+---+---+
            second byte is:
            +---+---+---+---+---+---+---+---+
            | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
            +---+---+---+---+---+---+---+---+
            third byte is:
            +---+---+---+---+---+---+---+---+
            | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
            +---+---+---+---+---+---+---+---+


        The UTF format:
        Writes a string to the underlying output stream using UTF-8
        encoding in a machine-independent manner.
        <p>
        First, two bytes are written to the output stream as if by the
        <code>writeShort</code> method giving the number of bytes to
        follow. This value is the number of bytes actually written out,
        not the length of the string. Following the length, each character
        of the string is output, in sequence, using the UTF-8 encoding
        for the character.
        @exception  IOException  if an I/O error occurs.
        @since      JDK1.0


      @exception IOException thrown by writeUTF

      @see java.io.DataInputStream

    */
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(isNull);

        if (isNull()) {
            return;
        }

        //
        // This handles the case that a CHAR or VARCHAR value was populated from
        // a user Clob.
        //
        if ( _clobValue != null )
        {
            writeClobUTF( out );
            return;
        }

        String lvalue = null;
        char[] data = null;

        int strlen = rawLength;
        boolean isRaw;

        if (strlen < 0) {
            lvalue = value;
            strlen = lvalue.length();
            isRaw = false;
        } else {
            data = rawData;
            isRaw = true;
        }

        // byte length will always be at least string length
        int utflen = strlen;

        for (int i = 0 ; (i < strlen) && (utflen <= 65535); i++)
        {
            int c = isRaw ? data[i] : lvalue.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F))
            {
                // 1 byte for character
            }
            else if (c > 0x07FF)
            {
                utflen += 2; // 3 bytes for character
            }
            else
            {
                utflen += 1; // 2 bytes for character
            }
        }

        StreamHeaderGenerator header = getStreamHeaderGenerator();
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(!header.expectsCharCount());
        }
        // Generate the header, write it to the destination stream, write the
        // user data and finally write an EOF-marker is required.
        header.generateInto(out, utflen);
        writeUTF(out, strlen, isRaw, null );
        header.writeEOF(out, utflen);
    }

    /**
     * Writes the user data value to a stream in the modified UTF-8 format.
     *
     * @param out destination stream
     * @param strLen string length of the value
     * @param isRaw {@code true} if the source is {@code rawData}, {@code false}
     *      if the source is {@code value}
     * @param characterReader Reader from _clobValue if it exists
     * @throws IOException if writing to the destination stream fails
     */
    private void writeUTF(ObjectOutput out, int strLen,
                          final boolean isRaw, Reader characterReader)
            throws IOException {
        // Copy the source reference into a local variable (optimization).
        final char[] data = isRaw ? rawData : null;
        final String lvalue = isRaw ? null : value;

        // Iterate through the value and write it as modified UTF-8.
        for (int i = 0 ; i < strLen ; i++) {
            int c;

            if ( characterReader != null ) { c = characterReader.read(); }
            else { c = isRaw ? data[i] : lvalue.charAt(i); }

            writeUTF(out, c);
        }
    }

    /**
     * Write a single character to a stream in the modified UTF-8 format.
     *
     * @param out the destination stream
     * @param c the character to write
     * @throws IOException if writing to the destination stream fails
     */
    private static void writeUTF(ObjectOutput out, int c) throws IOException {
        if ((c >= 0x0001) && (c <= 0x007F))
        {
            out.write(c);
        }
        else if (c > 0x07FF)
        {
            out.write(0xE0 | ((c >> 12) & 0x0F));
            out.write(0x80 | ((c >>  6) & 0x3F));
            out.write(0x80 | ((c >>  0) & 0x3F));
        }
        else
        {
            out.write(0xC0 | ((c >>  6) & 0x1F));
            out.write(0x80 | ((c >>  0) & 0x3F));
        }
    }

    /**
     * Writes the header and the user data for a CLOB to the destination stream.
     *
     * @param out destination stream
     * @throws IOException if writing to the destination stream fails
     */
    protected final void writeClobUTF(ObjectOutput out)
            throws IOException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(!isNull());
            SanityManager.ASSERT(stream == null, "Stream not null!");
        }

        boolean  isUserClob = ( _clobValue != null );

        try {

            boolean isRaw = rawLength >= 0;
            // Assume isRaw, update afterwards if required.
            int strLen = rawLength;
            if (!isRaw) {
                if ( isUserClob ) { strLen = rawGetClobLength(); }
                else { strLen = value.length(); }
            }
            // Generate the header and invoke the encoding routine.
            StreamHeaderGenerator header = getStreamHeaderGenerator();
            int toEncodeLen = header.expectsCharCount() ? strLen : -1;
            header.generateInto(out, toEncodeLen);

            Reader characterReader = null;
            if ( isUserClob ) { characterReader = _clobValue.getCharacterStream(); }

            writeUTF(out, strLen, isRaw, characterReader );
            header.writeEOF(out, toEncodeLen);

            if ( isUserClob ) { characterReader.close(); }
        }
        catch (SQLException se)
        {

            throw new IOException( se.getMessage(), se);
        }
    }


    private void setRaw(char[] data, int length)
    {
        rawData = data;
        rawLength = length;
        isNull = evaluateNull();
    }

    /**
     * Reads in a string from the specified data input stream. The
     * string has been encoded using a modified UTF-8 format.
     * <p>
     * The first two bytes are read as if by
     * <code>readUnsignedShort</code>. This value gives the number of
     * following bytes that are in the encoded string, not
     * the length of the resulting string. The following bytes are then
     * interpreted as bytes encoding characters in the UTF-8 format
     * and are converted into characters.
     * <p>
     * This method blocks until all the bytes are read, the end of the
     * stream is detected, or an exception is thrown.
     *
     * @param      in   a data input stream.
     * @exception  EOFException            if the input stream reaches the end
     *               before all the bytes.
     * @exception  IOException             if an I/O error occurs.
     * @exception  UTFDataFormatException  if the bytes do not represent a
     *               valid UTF-8 encoding of a Unicode string.
     * @see        java.io.DataInputStream#readUnsignedShort()

     * @see java.io.Externalizable#readExternal
     */
    public void readExternalFromArray(ArrayInputStream in)
        throws IOException
    {
        resetForMaterialization();
        int utfLen = (((in.read() & 0xFF) << 8) | (in.read() & 0xFF));
        if (rawData == null || rawData.length < utfLen) {
            // This array may be as much as three times too big. This happens
            // if the content is only 3-byte characters (i.e. CJK).
            // TODO: Decide if a threshold should be introduced, where the
            //       content is copied to a smaller array if the number of
            //       unused array positions exceeds the threshold.
            rawData = new char[utfLen];
        }
        arg_passer[0]        = rawData;
        int length = in.readDerbyUTF(arg_passer, utfLen);
        setRaw(arg_passer[0], length);
    }
    char[][] arg_passer = new char[1][];

    /**
     * Reads a CLOB from the source stream and materializes the value in a
     * character array.
     *
     * @param in source stream
     * @param charLen the char length of the value, or {@code 0} if unknown
     * @throws IOException if reading from the source fails
     */
    protected void readExternalClobFromArray(ArrayInputStream in, int charLen)
            throws IOException {
        resetForMaterialization();
        if (rawData == null || rawData.length < charLen) {
            rawData = new char[charLen];
        }
        arg_passer[0] = rawData;
        int length = in.readDerbyUTF(arg_passer, 0);
        setRaw(arg_passer[0], length);
    }

    /**
     * Resets state after materializing value from an array.
     */
    private void resetForMaterialization() {
        value  = null;
        stream = null;
        cKey = null;
        isNull = evaluateNull();
    }

    public void readExternal(ObjectInput in) throws IOException {
        isNull = in.readBoolean();

        if (isNull()) {
            return;
        }

        // Read the stored length in the stream header.
        int utflen = in.readUnsignedShort();
        if (utflen == 0) {
            resetForMaterialization();
            char str[] = new char[1];
            setRaw(str, utflen);
        }
        else {
            readExternal(in, utflen, 0);
        }
        isNull = evaluateNull();
    }

    /**
     * Restores the data value from the source stream, materializing the value
     * in memory.
     *
     * @param in the source stream
     * @param utflen the byte length, or {@code 0} if unknown
     * @param knownStrLen the char length, or {@code 0} if unknown
     * @throws UTFDataFormatException if an encoding error is detected
     * @throws IOException if reading the stream fails
     */
    protected void readExternal(ObjectInput in, int utflen,
                                final int knownStrLen)
            throws IOException {
        int requiredLength;
        // minimum amount that is reasonable to grow the array
        // when we know the array needs to growby at least one
        // byte but we dont want to grow by one byte as that
        // is not performant
        int minGrowBy = growBy();
        if (utflen != 0)
        {
            // the object was not stored as a streaming column
            // we know exactly how long it is
            requiredLength = utflen;
        }
        else
        {
            // the object was stored as a streaming column
            // and we have a clue how much we can read unblocked
            // OR
            // The original string was a 0 length string.
            requiredLength = in.available();
            if (requiredLength < minGrowBy)
                requiredLength = minGrowBy;
        }

        char str[];
        if ((rawData == null) || (requiredLength > rawData.length)) {

            str = new char[requiredLength];
        } else {
            str = rawData;
        }
        int arrayLength = str.length;

        // Set these to null to allow GC of the array if required.
        rawData = null;
        resetForMaterialization();

        int count = 0;
        int strlen = 0;

        readingLoop:
        while (((strlen < knownStrLen) || (knownStrLen == 0)) &&
                ((count < utflen) || (utflen == 0)))
        {
            int c;

            try {

                c = in.readUnsignedByte();
            } catch (EOFException eof) {
                if (utflen != 0)
                    throw new EOFException();

                // This is the case for a 0 length string.
                // OR the string was originally streamed in
                // which puts a 0 for utflen but no trailing
                // E0,0,0 markers.
                break;
            }

            //if (c == -1)      // read EOF
            //{
            //  if (utflen != 0)
            //      throw new EOFException();

            //  break;
            //}

            // change it to an unsigned byte
            //c &= 0xFF;

            if (strlen >= arrayLength) // the char array needs to be grown
            {
                int growby = in.available();
                // We know that the array needs to be grown by at least one.
                // However, even if the input stream wants to block on every
                // byte, we don't want to grow by a byte at a time.
                // Note, for large data (clob > 32k), it is performant
                // to grow the array by atleast 4k rather than a small amount
                // Even better maybe to grow by 32k but then may be
                // a little excess(?) for small data.
                // hopefully in.available() will give a fair
                // estimate of how much data can be read to grow the
                // array by larger and necessary chunks.
                // This performance issue due to
                // the slow growth of this array was noticed since inserts
                // on clobs was taking a really long time as
                // the array here grew previously by 64 bytes each time
                // till stream was drained.  (Derby-302)
                // for char, growby 64 seems reasonable, but for varchar
                // clob 4k or 32k is performant and hence
                // growBy() is override correctly to ensure this
                if (growby < minGrowBy)
                    growby = minGrowBy;

                int newstrlength = arrayLength + growby;
                char oldstr[] = str;
                str = new char[newstrlength];

                System.arraycopy(oldstr, 0, str, 0, arrayLength);
                arrayLength = newstrlength;
            }

            /// top fours bits of the first unsigned byte that maps to a
            //  1,2 or 3 byte character
            //
            // 0000xxxx - 0 - 1 byte char
            // 0001xxxx - 1 - 1 byte char
            // 0010xxxx - 2 - 1 byte char
            // 0011xxxx - 3 - 1 byte char
            // 0100xxxx - 4 - 1 byte char
            // 0101xxxx - 5 - 1 byte char
            // 0110xxxx - 6 - 1 byte char
            // 0111xxxx - 7 - 1 byte char
            // 1000xxxx - 8 - error
            // 1001xxxx - 9 - error
            // 1010xxxx - 10 - error
            // 1011xxxx - 11 - error
            // 1100xxxx - 12 - 2 byte char
            // 1101xxxx - 13 - 2 byte char
            // 1110xxxx - 14 - 3 byte char
            // 1111xxxx - 15 - error

            int char2, char3;
            char actualChar;
            if ((c & 0x80) == 0x00)
            {
                // one byte character
                count++;
                actualChar = (char) c;
            }
            else if ((c & 0x60) == 0x40) // we know the top bit is set here
            {
                // two byte character
                count += 2;
                if (utflen != 0 && count > utflen)
                    throw new UTFDataFormatException();
                char2 = in.readUnsignedByte();
                if ((char2 & 0xC0) != 0x80)
                    throw new UTFDataFormatException();
                actualChar = (char)(((c & 0x1F) << 6) | (char2 & 0x3F));
            }
            else if ((c & 0x70) == 0x60) // we know the top bit is set here
            {
                // three byte character
                count += 3;
                if (utflen != 0 && count > utflen)
                    throw new UTFDataFormatException();
                char2 = in.readUnsignedByte();
                char3 = in.readUnsignedByte();
                if ((c == 0xE0) && (char2 == 0) && (char3 == 0)
                    && (utflen == 0))
                {
                    // we reached the end of a long string,
                    // that was terminated with
                    // (11100000, 00000000, 00000000)
                    break;
                }

                if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                    throw new UTFDataFormatException();


                actualChar = (char)(((c & 0x0F) << 12) |
                                           ((char2 & 0x3F) << 6) |
                                           ((char3 & 0x3F) << 0));
            }
            else {
                throw new UTFDataFormatException(
                        "Invalid code point: " + Integer.toHexString(c));
            }

            str[strlen++] = actualChar;
        }


        setRaw(str, strlen);
    }

    /**
     * returns the reasonable minimum amount by
     * which the array can grow . See readExternal.
     * when we know that the array needs to grow by at least
     * one byte, it is not performant to grow by just one byte
     * instead this amount is used to provide a resonable growby size.
     * @return minimum reasonable growby size
     */
    protected int growBy()
    {
        return GROWBY_FOR_CHAR;  //seems reasonable for a char
    }
    /**
     * @see Storable#restoreToNull
     *
     */
    public void restoreToNull()
    {
        value = null;
        _clobValue = null;
        stream = null;
        rawLength = -1;
        cKey = null;
        isNull = true;
    }

    /**
        @exception StandardException thrown on error
     */
    public boolean compare(int op,
                           DataValueDescriptor other,
                           boolean orderedNulls,
                           boolean unknownRV)
        throws StandardException
    {
        if (!orderedNulls)      // nulls are unordered
        {
            if (this.isNull() || ((DataValueDescriptor) other).isNull())
                return unknownRV;
        }

        /* When comparing String types to non-string types, we always
         * convert the string type to the non-string type.
         */
        if (! (other instanceof SQLChar))
        {
            return other.compare(flip(op), this, orderedNulls, unknownRV);
        }

        /* Do the comparison */
        return super.compare(op, other, orderedNulls, unknownRV);
    }

    /**
        @exception StandardException thrown on error
     */
    @SuppressFBWarnings(value="RV_NEGATING_RESULT_OF_COMPARETO", justification="Using compare method from dominant type")
    public int compare(DataValueDescriptor other) throws StandardException
    {
        /* Use compare method from dominant type, negating result
         * to reflect flipping of sides.
         */
        if (typePrecedence() < other.typePrecedence())
        {
            return - (other.compare(this));
        }

        // stringCompare deals with null as comparable and smallest
        return stringCompare(this, (StringDataValue)other);
    }

    /*
     * CloneableObject interface
     */

    /**
     *  Shallow clone a StreamStorable without objectifying.  This is used to
     *  avoid unnecessary objectifying of a stream object.  The only
     *  difference of this method from cloneValue is this method does not
     *  objectify a stream.
     */
    public DataValueDescriptor cloneHolder() {
        if ((stream == null) && (_clobValue == null)) {
            return cloneValue(false);
        }

        SQLChar self = (SQLChar) getNewNull();
        self.copyState(this);

        return self;
    }

    /*
     * DataValueDescriptor interface
     */

    /** @see DataValueDescriptor#cloneValue */
    public DataValueDescriptor cloneValue(boolean forceMaterialization)
    {
        try
        {
            SQLChar ret = new SQLChar(getString());
            ret.setSqlCharSize(sqlCharSize);
            return ret;
        }
        catch (StandardException se)
        {
            if (SanityManager.DEBUG)
                SanityManager.THROWASSERT("Unexpected exception", se);
            return null;
        }
    }

    /**
     * @see DataValueDescriptor#getNewNull
     *
     */
    public DataValueDescriptor getNewNull()
    {
        return new SQLChar();
    }

    /** @see StringDataValue#getValue(RuleBasedCollator) */
    public StringDataValue getValue(RuleBasedCollator collatorForComparison)
    {
        if (collatorForComparison == null)
        {//null collatorForComparison means use UCS_BASIC for collation
            return this;
        } else {
            //non-null collatorForComparison means use collator sensitive
            //implementation of SQLChar
             CollatorSQLChar s = new CollatorSQLChar(collatorForComparison);
             s.copyState(this);
             return s;
        }
    }

    /**
     * @see DataValueDescriptor#setValueFromResultSet
     *
     * @exception SQLException      Thrown on error
     */
    public final void setValueFromResultSet(ResultSet resultSet, int colNumber,
                                      boolean isNullable)
        throws SQLException
    {
            setValue(resultSet.getString(colNumber));
    }

    /**
        Set the value into a PreparedStatement.
    */
    public final void setInto(
    PreparedStatement   ps,
    int                 position)
        throws SQLException, StandardException
    {
        ps.setString(position, getString());
        isNull = evaluateNull();
    }



    public void setValue(Clob theValue)
    {
        stream = null;
        rawLength = -1;
        cKey = null;
        value = null;

        _clobValue = theValue;
        isNull = evaluateNull();
    }

    public void setValue(String theValue)
    {
        stream = null;
        rawLength = -1;
        cKey = null;
        _clobValue = null;

        value = theValue;
        isNull = evaluateNull();
    }

    public void setValue(boolean theValue) throws StandardException
    {
        setValue(Boolean.toString(theValue));
    }

    public void setValue(int theValue)  throws StandardException
    {
        setValue(Integer.toString(theValue));
    }

    public void setValue(double theValue)  throws StandardException
    {
        setValue(Double.toString(theValue));
    }

    public void setValue(float theValue)  throws StandardException
    {
        setValue(Float.toString(theValue));
    }

    public void setValue(short theValue)  throws StandardException
    {
        setValue(Short.toString(theValue));
    }

    public void setValue(long theValue)  throws StandardException
    {
        setValue(Long.toString(theValue));
    }

    public void setValue(byte theValue)  throws StandardException
    {
        setValue(Byte.toString(theValue));
    }

    public void setValue(byte[] theValue) throws StandardException
    {
        if (theValue == null)
        {
            restoreToNull();
            return;
        }

        /*
        ** We can't just do a new String(theValue)
        ** because that method assumes we are converting
        ** ASCII and it will take on char per byte.
        ** So we need to convert the byte array to a
        ** char array and go from there.
        **
        ** If we have an odd number of bytes pad out.
        */
        int mod = (theValue.length % 2);
        int len = (theValue.length/2) + mod;
        char[] carray = new char[len];
        int cindex = 0;
        int bindex = 0;

        /*
        ** If we have a left over byte, then get
        ** that now.
        */
        if (mod == 1)
        {
            carray[--len] = (char)(theValue[theValue.length - 1] << 8);
        }

        for (; cindex < len; bindex+=2, cindex++)
        {
            carray[cindex] = (char)((theValue[bindex] << 8) |
                                (theValue[bindex+1] & 0x00ff));
        }

        setValue(new String(carray));
    }

    /**
        Only to be called when an application through JDBC is setting a
        SQLChar to a java.math.BigDecimal.
    */
    public void setBigDecimal(Number bigDecimal)  throws StandardException
    {
        if (bigDecimal == null)
            setToNull();
        else
            setValue(bigDecimal.toString());
    }

    /** @exception StandardException        Thrown on error */
    public void setValue(Date theValue, Calendar cal) throws StandardException
    {
        String strValue = null;
        if( theValue != null)
        {
            if( cal == null)
                strValue = theValue.toString();
            else
            {
                cal.setTime( theValue);
                StringBuffer sb = new StringBuffer();
                formatJDBCDate( cal, sb);
                strValue= sb.toString();
            }
        }
        setValue( strValue);
    }

    /** @exception StandardException        Thrown on error */
    public void setValue(Time theValue, Calendar cal) throws StandardException
    {
        String strValue = null;
        if( theValue != null)
        {
            if( cal == null)
                strValue = theValue.toString();
            else
            {
                cal.setTime( theValue);
                StringBuffer sb = new StringBuffer();
                formatJDBCTime( cal, sb);
                strValue= sb.toString();
            }
        }
        setValue( strValue);
    }

    /** @exception StandardException        Thrown on error */
    public void setValue(
    Timestamp   theValue,
    Calendar    cal)
        throws StandardException
    {
        String strValue = null;
        if( theValue != null)
        {
            if( cal == null)
                strValue = theValue.toString();
            else
            {
                cal.setTime( theValue);
                StringBuffer sb = new StringBuffer();
                formatJDBCDate( cal, sb);
                sb.append( ' ');
                formatJDBCTime( cal, sb);
                sb.append('.');

                int nanos = theValue.getNanos();

                if (nanos == 0)
                {
                    // Add a single zero after the decimal point to match
                    // the format from Timestamp.toString().
                    sb.append('0');
                }
                else if (nanos > 0)
                {
                    String nanoString = Integer.toString(nanos);
                    int len = nanoString.length();

                    // Add leading zeros if nanoString is shorter than
                    // MAX_FRACTION_DIGITS.
                    for (int i = len;
                         i < SQLTimestamp.MAX_FRACTION_DIGITS; i++)
                    {
                        sb.append('0');
                    }

                    // Remove trailing zeros to match the format from
                    // Timestamp.toString().
                    while (nanoString.charAt(len - 1) == '0') {
                        len--;
                    }

                    sb.append(nanoString.substring(0, len));
                }
                strValue= sb.toString();
            }
        }
        setValue( strValue);
    }

    private void formatJDBCDate( Calendar cal, StringBuffer sb)
    {
        SQLDate.dateToString( cal.get( Calendar.YEAR),
                              cal.get( Calendar.MONTH) - Calendar.JANUARY + 1,
                              cal.get( Calendar.DAY_OF_MONTH),
                              sb);
    }

    private void formatJDBCTime( Calendar cal, StringBuffer sb)
    {
        SQLTime.timeToString(
            cal.get(Calendar.HOUR_OF_DAY),
            cal.get(Calendar.MINUTE),
            cal.get(Calendar.SECOND),
            sb);
    }

    /**
     * Set the value from the stream which is in the on-disk format.
     * @param theStream On disk format of the stream
     * @param valueLength length of the logical value in characters, or
     *      <code>DataValueDescriptor.UNKNOWN_LOGICAL_LENGTH</code>
     */
    public final void setValue(InputStream theStream, int valueLength)
    {
        setStream(theStream);
        isNull = evaluateNull();
    }

    /**
     * Allow any Java type to be cast to a character type using
     * Object.toString.
     * @see DataValueDescriptor#setObjectForCast
     *
     * @exception StandardException
     *                thrown on failure
     */
    public void setObjectForCast(
    Object  theValue,
    boolean instanceOfResultType,
    String  resultTypeClassName)
        throws StandardException
    {
        if (theValue == null)
        {
            setToNull();
            return;
        }

        if ("java.lang.String".equals(resultTypeClassName))
            setValue(theValue.toString());
        else {
            super.setObjectForCast(
                theValue, instanceOfResultType, resultTypeClassName);
            isNull = evaluateNull();
        }
    }

    protected void setFrom(DataValueDescriptor theValue)
        throws StandardException
    {
        if ( theValue instanceof SQLChar )
        {
            SQLChar that = (SQLChar) theValue;

            if ( that._clobValue != null )
            {
                setValue( that._clobValue );
                return;
            }
        }
        setValue(theValue.getString());
    }

    /**
     * Normalization method - this method may be called when putting
     * a value into a SQLChar, for example, when inserting into a SQLChar
     * column.  See NormalizeResultSet in execution.
     *
     * @param desiredType   The type to normalize the source column to
     * @param source        The value to normalize
     *
     *
     * @exception StandardException             Thrown for null into
     *                                          non-nullable column, and for
     *                                          truncation error
     */

    public void normalize(
                DataTypeDescriptor desiredType,
                DataValueDescriptor source)
                    throws StandardException
    {

        normalize(desiredType, source.getString());

    }

    protected void normalize(DataTypeDescriptor desiredType, String sourceValue)
        throws StandardException
    {


        int desiredWidth = desiredType.getMaximumWidth();
        int sourceWidth = sourceValue.length();

        /*
        ** If the input is already the right length, no normalization is
        ** necessary - just return the source.
        */
        if (sourceWidth == desiredWidth) {
            setValue(sourceValue);
            return;
        }

        /*
        ** If the input is shorter than the desired type, construct a new
        ** SQLChar padded with blanks to the right length.
        */
        if (sourceWidth < desiredWidth)
        {
            setToNull();

            char[] ca;
            if ((rawData == null) || (desiredWidth > rawData.length)) {

                ca = rawData = new char[desiredWidth];
            } else {
                ca = rawData;
            }

            sourceValue.getChars(0, sourceWidth, ca, 0);
            SQLChar.appendBlanks(ca, sourceWidth, desiredWidth - sourceWidth);

            setRaw(rawData, desiredWidth);

            return;
        }

        /*
        ** Check whether any non-blank characters will be truncated.
        */
        hasNonBlankChars(sourceValue, desiredWidth, sourceWidth);

        /*
        ** No non-blank characters will be truncated.  Truncate the blanks
        ** to the desired width.
        */

        String truncatedString = sourceValue.substring(0, desiredWidth);
        setValue(truncatedString);
    }

    /*
    ** Method to check for truncation of non blank chars.
    */
    protected final void hasNonBlankChars(String source, int start, int end)
        throws StandardException
    {
        /*
        ** Check whether any non-blank characters will be truncated.
        */
        for (int posn = start; posn < end; posn++)
        {
            if (source.charAt(posn) != ' ')
            {
                throw StandardException.newException(
                    SQLState.LANG_STRING_TRUNCATION,
                    getTypeName(),
                    StringUtil.formatForPrint(source),
                    String.valueOf(start));
            }
        }
    }

    ///////////////////////////////////////////////////////////////
    //
    // VariableSizeDataValue INTERFACE
    //
    ///////////////////////////////////////////////////////////////

    /**
     * Set the width of the to the desired value.  Used
     * when CASTing.  Ideally we'd recycle normalize(), but
     * the behavior is different (we issue a warning instead
     * of an error, and we aren't interested in nullability).
     *
     * @param desiredWidth  the desired length
     * @param desiredScale  the desired scale (ignored)
     * @param errorOnTrunc  throw an error on truncation
     *
     * @exception StandardException     Thrown when errorOnTrunc
     *      is true and when a shrink will truncate non-white
     *      spaces.
     */
    public void setWidth(int desiredWidth,
                                    int desiredScale, // Ignored
                                    boolean errorOnTrunc)
                            throws StandardException
    {
        int sourceWidth;

        /*
        ** If the input is NULL, nothing to do.
        */
        if ( (_clobValue == null ) && (getString() == null) )
        {
            return;
        }

        sourceWidth = getLength();

        /*
        ** If the input is shorter than the desired type, construct a new
        ** SQLChar padded with blanks to the right length.  Only
        ** do this if we have a SQLChar -- SQLVarchars don't
        ** pad.
        */
        if (sourceWidth < desiredWidth)
        {
            if (!(this instanceof SQLVarchar))
            {
                StringBuilder    strBuilder;

                strBuilder = new StringBuilder(getString());

                for ( ; sourceWidth < desiredWidth; sourceWidth++)
                {
                    strBuilder.append(' ');
                }

                setValue(new String(strBuilder));
            }
        }
        else if (sourceWidth > desiredWidth && desiredWidth > 0)
        {
            /*
            ** Check whether any non-blank characters will be truncated.
            */
            try {
                hasNonBlankChars(getString(), desiredWidth, sourceWidth);
            } catch (StandardException se) {
                if (errorOnTrunc) {
                    throw se;
                }

                // Generate a truncation warning, as specified in SQL:2003,
                // part 2, 6.12 <cast specification>, general rules 10)c)2)
                // and 11)c)2).

                // Data size and transfer size need to be in bytes per
                // DataTruncation javadoc.
                String source = getString();
                int transferSize = getUTF8Length(source, 0, desiredWidth);
                int dataSize = transferSize +
                        getUTF8Length(source, desiredWidth, source.length());

                DataTruncation warning = new DataTruncation(
                    -1,     // column index is unknown
                    false,  // parameter
                    true,   // read
                    dataSize,
                    transferSize, se);

                StatementContext statementContext = (StatementContext)
                    ContextService.getContext(ContextId.LANG_STATEMENT);
                if (statementContext != null) {
                    // In some cases, the activation will be null on the context,
                    // which resulted in NPE when trying to add warning (DB-1288).
                    // When this happens, we have no choice but to skip the warning.
                    // We can address this later when we more deeply look into
                    // error and warning propagation.
                    Activation activation = statementContext.getActivation();
                    if (activation != null) {
                        activation.getResultSet().addWarning(warning);
                    }
                }

                String warningMessage = String.format("Truncated a string source=%s, sourceWidth=%d, desiredWidth=%d, type=%s",
                        source, sourceWidth, desiredWidth, this.getTypeName());
                logger.warn(warningMessage);
            }

            /*
            ** Truncate to the desired width.
            */
            setValue(getString().substring(0, desiredWidth));
        }
    }

    /**
     * Get the number of bytes needed to represent a string in modified
     * UTF-8, which is the encoding used by {@code writeExternal()} and
     * {@code writeUTF()}.
     *
     * @param string the string whose length to calculate
     * @param start start index (inclusive)
     * @param end end index (exclusive)
     */
    private int getUTF8Length(String string, int start, int end)
            throws StandardException {
        CounterOutputStream cs = new CounterOutputStream();

        try {
            FormatIdOutputStream out = new FormatIdOutputStream(cs);
            for (int i = start; i < end; i++) {
                writeUTF(out, string.charAt(i));
            }
            out.close();
        } catch (IOException ioe) {
            throw StandardException.newException(
                    SQLState.LANG_IO_EXCEPTION, ioe, ioe.toString());
        }

        return cs.getCount();
    }

    /*
    ** SQL Operators
    */

    /**
     * The = operator as called from the language module, as opposed to
     * the storage module.
     *
     * @param left          The value on the left side of the =
     * @param right         The value on the right side of the =
     *
     * @return  A SQL boolean value telling whether the two parameters are equal
     *
     * @exception StandardException     Thrown on error
     */

    public BooleanDataValue equals(DataValueDescriptor left,
                             DataValueDescriptor right)
                                throws StandardException
    {
        boolean comparison;

        if ((left instanceof SQLChar) && (right instanceof SQLChar))
        {
            comparison = stringCompare((SQLChar) left, (SQLChar) right) == 0;
        }
        else
        {
            comparison = stringCompare(left.getString(),
                                       right.getString()) == 0;
        }

        return SQLBoolean.truthValue(left,
                                     right,
                                     comparison);
    }

    /**
     * The <> operator as called from the language module, as opposed to
     * the storage module.
     *
     * @param left          The value on the left side of the <>
     * @param right         The value on the right side of the <>
     *
     * @return  A SQL boolean value telling whether the two parameters
     * are not equal
     *
     * @exception StandardException     Thrown on error
     */

    public BooleanDataValue notEquals(DataValueDescriptor left,
                             DataValueDescriptor right)
                                throws StandardException
    {
        boolean comparison;

        if ((left instanceof SQLChar) && (right instanceof SQLChar))
        {
            comparison = stringCompare((SQLChar) left, (SQLChar) right) != 0;
        }
        else
        {
            comparison = stringCompare(left.getString(),
                                       right.getString()) != 0;
        }

        return SQLBoolean.truthValue(left,
                                     right,
                                     comparison);
    }

    /**
     * The < operator as called from the language module, as opposed to
     * the storage module.
     *
     * @param left          The value on the left side of the <
     * @param right         The value on the right side of the <
     *
     * @return  A SQL boolean value telling whether the first operand is
     *          less than the second operand
     *
     * @exception StandardException     Thrown on error
     */

    public BooleanDataValue lessThan(DataValueDescriptor left,
                             DataValueDescriptor right)
                                throws StandardException
    {
        boolean comparison;

        if ((left instanceof SQLChar) && (right instanceof SQLChar))
        {
            comparison = stringCompare((SQLChar) left, (SQLChar) right) < 0;
        }
        else
        {
            comparison = stringCompare(left.getString(),
                                       right.getString()) < 0;
        }

        return SQLBoolean.truthValue(left,
                                     right,
                                     comparison);
    }

    /**
     * The > operator as called from the language module, as opposed to
     * the storage module.
     *
     * @param left          The value on the left side of the >
     * @param right         The value on the right side of the >
     *
     * @return  A SQL boolean value telling whether the first operand is
     *          greater than the second operand
     *
     * @exception StandardException     Thrown on error
     */

    public BooleanDataValue greaterThan(DataValueDescriptor left,
                             DataValueDescriptor right)
                                throws StandardException
    {
        boolean comparison;

        if ((left instanceof SQLChar) && (right instanceof SQLChar))
        {
            comparison = stringCompare((SQLChar) left, (SQLChar) right) > 0;
        }
        else
        {
            comparison = stringCompare(left.getString(),
                                       right.getString()) > 0;
        }

        return SQLBoolean.truthValue(left,
                                     right,
                                     comparison);
    }

    /**
     * The <= operator as called from the language module, as opposed to
     * the storage module.
     *
     * @param left          The value on the left side of the <=
     * @param right         The value on the right side of the <=
     *
     * @return  A SQL boolean value telling whether the first operand is
     *          less than or equal to the second operand
     *
     * @exception StandardException     Thrown on error
     */

    public BooleanDataValue lessOrEquals(DataValueDescriptor left,
                             DataValueDescriptor right)
                                throws StandardException
    {
        boolean comparison;

        if ((left instanceof SQLChar) && (right instanceof SQLChar))
        {
            comparison = stringCompare((SQLChar) left, (SQLChar) right) <= 0;
        }
        else
        {
            comparison = stringCompare(left.getString(),
                                       right.getString()) <= 0;
        }

        return SQLBoolean.truthValue(left,
                                     right,
                                     comparison);
    }

    /**
     * The >= operator as called from the language module, as opposed to
     * the storage module.
     *
     * @param left          The value on the left side of the >=
     * @param right         The value on the right side of the >=
     *
     * @return  A SQL boolean value telling whether the first operand is
     *          greater than or equal to the second operand
     *
     * @exception StandardException     Thrown on error
     */

    public BooleanDataValue greaterOrEquals(DataValueDescriptor left,
                             DataValueDescriptor right)
                                throws StandardException
    {
        boolean comparison;

        if ((left instanceof SQLChar) && (right instanceof SQLChar))
        {
            comparison = stringCompare((SQLChar) left, (SQLChar) right) >= 0;
        }
        else
        {
            comparison = stringCompare(left.getString(),
                                       right.getString()) >= 0;
        }

        return SQLBoolean.truthValue(left,
                                     right,
                                     comparison);
    }

    /*
    ** Concatable interface
    */
    /**
     * This method implements the char_length function for char.
     *
     * @param result    The result of a previous call to this method, null
     *                  if not called yet
     *
     * @return  A SQLInteger containing the length of the char value
     *
     * @exception StandardException     Thrown on error
     *
     * @see ConcatableDataValue#charLength(NumberDataValue)
     */
    public NumberDataValue charLength(NumberDataValue result)
                            throws StandardException
    {
        if (result == null)
        {
            result = new SQLInteger();
        }

        if (this.isNull())
        {
            result.setToNull();
            return result;
        }

        result.setValue(this.getLength());
        return result;
    }

    /**
     * @see StringDataValue#concatenate
     *
     * @exception StandardException     Thrown on error
     */
    public StringDataValue concatenate(
                StringDataValue leftOperand,
                StringDataValue rightOperand,
                StringDataValue result)
        throws StandardException
    {
        if (result == null)
        {
            result = (StringDataValue) getNewNull();
        }

        if (leftOperand.isNull() || leftOperand.getString() == null ||
            rightOperand.isNull() || rightOperand.getString() == null)
        {
            result.setToNull();
            return result;
        }

        result.setValue(
                leftOperand.getString() + rightOperand.getString());

        return result;
    }

    /**
     * return a string that repeats the leftOperand number of times specified by the rightOperand
     * @param leftOperand  the string to be repeated
     * @param rightOperand  the number of times to repeat, it should be a number >= 0. When it is 0,
     *                      an empty string will be returned if leftOperand is not null.
     * @param result  the result string
     * @return
     * @throws StandardException
     */
    public StringDataValue repeat(StringDataValue leftOperand,
                                  NumberDataValue rightOperand,
                                  StringDataValue result) throws StandardException {
        if (result == null)
        {
            result = (StringDataValue) getNewNull();
        }

        if (leftOperand == null || leftOperand.isNull() || leftOperand.getString() == null)
        {
            result.setToNull();
            return result;
        }

        int repeatedTimes;
        if (rightOperand == null || rightOperand.isNull()) {
            throw StandardException.newException(
                    SQLState.LANG_INVALID_FUNCTION_ARGUMENT, "NULL", "REPEAT");
        } else {
            try {
                repeatedTimes = rightOperand.getInt();
            } catch (StandardException e) {
                throw StandardException.newException(
                        SQLState.LANG_INVALID_FUNCTION_ARGUMENT, rightOperand, "REPEAT");
            }

            if (repeatedTimes < 0) {
                throw StandardException.newException(
                        SQLState.LANG_INVALID_FUNCTION_ARGUMENT, rightOperand, "REPEAT");
            }
        }

        String val = leftOperand.getString();
        StringBuilder builder = new StringBuilder();

        for (int i=0; i<repeatedTimes; i++) {
            builder.append(val);
        }

        result.setValue(builder.toString());

        return result;
    }


    /**
     * This method implements the like function for char (with no escape value).
     *
     * @param pattern       The pattern to use
     *
     * @return  A SQL boolean value telling whether the first operand is
     *          like the second operand
     *
     * @exception StandardException     Thrown on error
     */
    public BooleanDataValue like(DataValueDescriptor pattern)
                                throws StandardException
    {
        Boolean likeResult;

        // note that we call getLength() because the length
        // of the char array may be different than the
        // length we should be using (i.e. getLength()).
        // see getCharArray() for more info
        char[] evalCharArray = getCharArray();
        char[] patternCharArray = ((StringDataValue)pattern).getCharArray();
        likeResult = Like.like(evalCharArray,
                               getLength(),
                                   patternCharArray,
                               pattern.getLength(),
                               null);

        return SQLBoolean.truthValue(this,
                                     pattern,
                                     likeResult);
    }

    /**
     * This method implements the like function for char with an escape value.
     *
     * @param pattern       The pattern to use
     *
     * @return  A SQL boolean value telling whether the first operand is
     *          like the second operand
     *
     * @exception StandardException     Thrown on error
     */

    public BooleanDataValue like(
                             DataValueDescriptor pattern,
                             DataValueDescriptor escape)
                                throws StandardException
    {
        Boolean likeResult;

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(
                             pattern instanceof StringDataValue &&
                             escape instanceof StringDataValue,
            "All three operands must be instances of StringDataValue");

        // ANSI states a null escape yields 'unknown' results
        //
        // This method is only called when we have an escape clause, so this
        // test is valid

        if (escape.isNull())
        {
            throw StandardException.newException(SQLState.LANG_ESCAPE_IS_NULL);
        }

        // note that we call getLength() because the length
        // of the char array may be different than the
        // length we should be using (i.e. getLength()).
        // see getCharArray() for more info
        char[] evalCharArray = getCharArray();
        char[] patternCharArray = ((SQLChar)pattern).getCharArray();
        char[] escapeCharArray = (((SQLChar) escape).getCharArray());
        int escapeLength = escape.getLength();

        if (escapeCharArray != null && escapeLength != 1 )
        {
            throw StandardException.newException(
                    SQLState.LANG_INVALID_ESCAPE_CHARACTER,
                    new String(escapeCharArray));
        }
        likeResult = Like.like(evalCharArray,
                               getLength(),
                                   patternCharArray,
                               pattern.getLength(),
                               escapeCharArray,
                               escapeLength,
                               null);

        return SQLBoolean.truthValue(this,
                                     pattern,
                                     likeResult);
    }

    /**
     * This method implements the locate function for char.
     * @param searchFrom    - The string to search from
     * @param start         - The position to search from in string searchFrom
     * @param result        - The object to return
     *
     * Note: use getString() to get the string to search for.
     *
     * @return  The position in searchFrom the fist occurrence of this.value.
     *              0 is returned if searchFrom does not contain this.value.
     * @exception StandardException     Thrown on error
     */
    public NumberDataValue locate(  ConcatableDataValue searchFrom,
                                    NumberDataValue start,
                                    NumberDataValue result)
                                    throws StandardException
    {
        int startVal;

        if( result == null )
        {
            result = new SQLInteger();
        }

        if( start.isNull() )
        {
            startVal = 1;
        }
        else
        {
            startVal = start.getInt();
        }

        if( searchFrom.isNull() || this.isNull())
        {
            result.setToNull();
            return result;
        }

        String mySearchFrom = searchFrom.getString();
        String mySearchFor = this.getString();

        /* the below 2 if conditions are to emulate DB2's behavior */
        if( startVal < 1 )
        {
            throw StandardException.newException(
                    SQLState.LANG_INVALID_PARAMETER_FOR_SEARCH_POSITION,
                    getString(), mySearchFrom,
                    startVal);
        }

        if(mySearchFor.isEmpty())
        {
            result.setValue( startVal );
            return result;
        }

        result.setValue( mySearchFrom.indexOf(mySearchFor, startVal - 1) + 1);
        return result;
    }

    /**
     * The SQL substr() function.
     *
     * @param start     Start of substr
     * @param length    Length of substr
     * @param result    The result of a previous call to this method,
     *                  null if not called yet.
     * @param maxLen    Maximum length of the result
     *
     * @return  A ConcatableDataValue containing the result of the substr()
     *
     * @exception StandardException     Thrown on error
     */
    public ConcatableDataValue substring(
                NumberDataValue start,
                NumberDataValue length,
                ConcatableDataValue result,
                int maxLen,
                boolean isFixedLength)
        throws StandardException
    {
        int startInt;
        int lengthInt;
        StringDataValue stringResult;

        if (result == null)
        {
            if (isFixedLength)
                result = new SQLChar();
            else
                result = getNewVarchar();
        }

        stringResult = (StringDataValue) result;

        /* The result is null if the receiver (this) is null or if the length
         * is negative.
         * We will return null, which is the only sensible thing to do.
         * (If user did not specify a length then length is not a user null.)
         */
        if (this.isNull() || start == null || start.isNull() ||
                (length != null && length.isNull()))
        {
            stringResult.setToNull();
            return stringResult;
        }

        startInt = start.getInt();

        // If length is not specified, make it till end of the string
        if (length != null)
        {
            lengthInt = length.getInt();
        }
        else lengthInt = maxLen - startInt + 1;

        /* DB2 Compatibility: Added these checks to match DB2. We currently
         * enforce these limits in both modes. We could do these checks in DB2
         * mode only, if needed, so leaving earlier code for out of range in
         * for now, though will not be exercised
         */
        if ((startInt <= 0 || lengthInt < 0 || startInt > maxLen ||
                lengthInt > maxLen - startInt + 1))
        {
            throw StandardException.newException(
                    SQLState.LANG_SUBSTR_START_OR_LEN_OUT_OF_RANGE);
        }

        if (lengthInt == 0)
        {
            stringResult.setValue("");
            return stringResult;
        }

        /* java substring() is 0 based */
        startInt--;

        if (startInt >= getLength()) {
            stringResult.setValue("");
            if (isFixedLength)
                stringResult.setWidth(lengthInt, -1, false);
        } else if (lengthInt > getLength() - startInt) {
            stringResult.setValue(getString().substring(startInt));
            if (isFixedLength)
                stringResult.setWidth(lengthInt, -1, false);
        } else {
            stringResult.setValue(
                getString().substring(startInt, startInt + lengthInt));
        }

        return stringResult;
    }

    public StringDataValue right(
            NumberDataValue length,
            StringDataValue result)
        throws StandardException {
        int lengthInt;
        StringDataValue stringResult;

        if (result == null)
        {
            result = getNewVarchar();
        }
        stringResult = (StringDataValue) result;

        if (this.isNull() || length.isNull() || length.getInt() < 0)
        {
            stringResult.setToNull();
            return stringResult;
        }

        lengthInt = length.getInt();
        if (lengthInt > getLength()) lengthInt = getLength();
        stringResult.setValue(getString().substring(getLength() - lengthInt));

        return stringResult;
    }

    public StringDataValue left(NumberDataValue length, StringDataValue result) throws StandardException
    {
        int lengthInt;
        StringDataValue stringResult;

        if (result == null)
        {
            result = getNewVarchar();
        }
        stringResult = (StringDataValue) result;

        if (this.isNull() || length.isNull() || length.getInt() < 0)
        {
            stringResult.setToNull();
            return stringResult;
        }

        lengthInt = length.getInt();
        if (lengthInt > getLength()) lengthInt = getLength();
        {
            stringResult.setValue(getString().substring(0, lengthInt));
        }

        return stringResult;
    }

    public ConcatableDataValue replace(
        StringDataValue fromStr,
        StringDataValue toStr,
        ConcatableDataValue result)
        throws StandardException
    {
        StringDataValue stringResult;

        if (result == null)
        {
            result = getNewVarchar();
        }

        stringResult = (StringDataValue) result;

        // The result is null if the receiver (this) is null
        // or either operand is null.

        if (this.isNull() ||
            fromStr.isNull() || fromStr.getString() == null ||
            toStr.isNull() || toStr.getString() == null)
        {
            stringResult.setToNull();
            return stringResult;
        }

        stringResult.setValue(getString().replace(fromStr.getString(), toStr.toString()));

        return stringResult;
    }

    /**
     * This function public for testing purposes.
     *
     * @param trimType  Type of trim (LEADING, TRAILING, or BOTH)
     * @param trimChar  Character to trim
     * @param source    String from which to trim trimChar
     *
     * @return A String containing the result of the trim.
     */
    private String trimInternal(int trimType, char trimChar, String source)
    {
        if (source == null) {
            return null;
        }

        int len = source.length();
        int start = 0;
        if (trimType == LEADING || trimType == BOTH)
        {
            for (; start < len; start++)
                if (trimChar != source.charAt(start))
                    break;
        }

        if (start == len)
            return "";

        int end = len - 1;
        if (trimType == TRAILING || trimType == BOTH)
        {
            for (; end >= 0; end--)
                if (trimChar != source.charAt(end))
                    break;
        }
        if (end == -1)
            return "";

        return source.substring(start, end + 1);
    }

    /**
     * @param trimType  Type of trim (LEADING, TRAILING, or BOTH)
     * @param trimChar  Character to trim from this SQLChar (may be null)
     * @param result    The result of a previous call to this method,
     *                  null if not called yet.
     *
     * @return A StringDataValue containing the result of the trim.
     */
    public StringDataValue ansiTrim(
    int             trimType,
    StringDataValue trimChar,
    StringDataValue result)
            throws StandardException
    {

        if (result == null)
        {
            result = getNewVarchar();
        }

        if (trimChar == null || trimChar.getString() == null)
        {
            result.setToNull();
            return result;
        }


        if (trimChar.getString().length() != 1)
        {
            throw StandardException.newException(
                    SQLState.LANG_INVALID_TRIM_CHARACTER, trimChar.getString());
        }

        char trimCharacter = trimChar.getString().charAt(0);

        result.setValue(trimInternal(trimType, trimCharacter, getString()));
        return result;
    }

    /** @see StringDataValue#upper
     *
     * @exception StandardException     Thrown on error
     */
    public StringDataValue upper(StringDataValue result)
                            throws StandardException
    {
        if (result == null)
        {
            result = (StringDataValue) getNewNull();
        }

        if (this.isNull())
        {
            result.setToNull();
            return result;
        }

        String upper = getString();
        upper = upper.toUpperCase(getLocale());
        result.setValue(upper);
        return result;
    }

    public StringDataValue upperWithLocale(StringDataValue leftOperand, StringDataValue rightOperand,
                                                StringDataValue result)
            throws StandardException
    {
        return upperLowerWithLocale(leftOperand, rightOperand, result, true);
    }

    public StringDataValue lowerWithLocale(StringDataValue leftOperand, StringDataValue rightOperand,
                                                StringDataValue result)
            throws StandardException
    {
        return upperLowerWithLocale(leftOperand, rightOperand, result, false);
    }

    private StringDataValue upperLowerWithLocale(StringDataValue leftOperand, StringDataValue rightOperand,
                                           StringDataValue result, boolean isUpper)
                            throws StandardException
    {
        if (result == null)
        {
            result = (StringDataValue) getNewNull();
        }

        if (leftOperand == null || leftOperand.isNull() || leftOperand.getString() == null)
        {
            result.setToNull();
            return result;
        }

        if (rightOperand == null || rightOperand.isNull() || rightOperand.getString() == null) {
            throw StandardException.newException(
                    SQLState.LANG_INVALID_FUNCTION_ARGUMENT, "NULL", isUpper?"UPPER":"LOWER");
        }
        String localeStr = rightOperand.getString();
        Locale locale = Locale.forLanguageTag(localeStr);
        if (!validLocale(locale)) {
            throw StandardException.newException(
                    SQLState.LANG_INVALID_FUNCTION_ARGUMENT, localeStr, isUpper?"UPPER":"LOWER");
        }
        String str = leftOperand.getString();
        str = isUpper?str.toUpperCase(locale):str.toLowerCase(locale);
        result.setValue(str);
        return result;
    }

    private boolean validLocale(Locale locale) {
        try {
            return !(locale.getISO3Language().equals("") || locale.getISO3Country().equals(""));
        } catch (MissingResourceException e) {
            return false;
        }
    }

    /** @see StringDataValue#lower
     *
     * @exception StandardException     Thrown on error
     */
    public StringDataValue lower(StringDataValue result)
                            throws StandardException
    {
        if (result == null)
        {
            result = (StringDataValue) getNewNull();
        }

        if (this.isNull())
        {
            result.setToNull();
            return result;
        }


        String lower = getString();
        lower = lower.toLowerCase(getLocale());
        result.setValue(lower);
        return result;
    }

    /*
     * DataValueDescriptor interface
     */

    /** @see DataValueDescriptor#typePrecedence */
    public int typePrecedence()
    {
        return TypeId.CHAR_PRECEDENCE;
    }

    /**
     * Compare two Strings using standard SQL semantics.
     *
     * @param op1               The first String
     * @param op2               The second String
     *
     * @return  -1 - op1 <  op2
     *           0 - op1 == op2
     *           1 - op1 > op2
     */
    protected static int stringCompare(String op1, String op2)
    {
        int         posn;
        char        leftchar;
        char        rightchar;
        int         leftlen;
        int         rightlen;
        int         retvalIfLTSpace;
        String      remainingString;
        int         remainingLen;

        /*
        ** By convention, nulls sort High, and null == null
        */
        if (op1 == null || op2 == null)
        {
            if (op1 != null)    // op2 == null
                return -1;
            if (op2 != null)    // op1 == null
                return 1;
            return 0;           // both null
        }
        /*
        ** Compare characters until we find one that isn't equal, or until
        ** one String or the other runs out of characters.
        */

        leftlen = op1.length();
        rightlen = op2.length();

        int shorterLen = leftlen < rightlen ? leftlen : rightlen;

        for (posn = 0; posn < shorterLen; posn++)
        {
            leftchar = op1.charAt(posn);
            rightchar = op2.charAt(posn);
            if (leftchar != rightchar)
            {
                if (leftchar < rightchar)
                    return -1;
                else
                    return 1;
            }
        }

        /*
        ** All the characters are equal up to the length of the shorter
        ** string.  If the two strings are of equal length, the values are
        ** equal.
        */
        if (leftlen == rightlen)
            return 0;

        /*
        ** One string is shorter than the other.  Compare the remaining
        ** characters in the longer string to spaces (the SQL standard says
        ** that in this case the comparison is as if the shorter string is
        ** padded with blanks to the length of the longer string.
        */
        if (leftlen > rightlen)
        {
            /*
            ** Remaining characters are on the left.
            */

            /* If a remaining character is less than a space,
             * return -1 (op1 < op2) */
            retvalIfLTSpace = -1;
            remainingString = op1;
            posn = rightlen;
            remainingLen = leftlen;
        }
        else
        {
            /*
            ** Remaining characters are on the right.
            */

            /* If a remaining character is less than a space,
             * return 1 (op1 > op2) */
            retvalIfLTSpace = 1;
            remainingString = op2;
            posn = leftlen;
            remainingLen = rightlen;
        }

        /* Look at the remaining characters in the longer string */
        for ( ; posn < remainingLen; posn++)
        {
            char    remainingChar;

            /*
            ** Compare the characters to spaces, and return the appropriate
            ** value, depending on which is the longer string.
            */

            remainingChar = remainingString.charAt(posn);

            if (remainingChar < ' ')
                return retvalIfLTSpace;
            else if (remainingChar > ' ')
                return -retvalIfLTSpace;
        }

        /* The remaining characters in the longer string were all spaces,
        ** so the strings are equal.
        */
        return 0;
    }

    /**
     * Compare two SQLChars.
     *
     * @exception StandardException     Thrown on error
     */
     protected int stringCompare(StringDataValue char1, StringDataValue char2)
         throws StandardException
     {
         return stringCompare(char1.getCharArray(), char1.getLength(),
                              char2.getCharArray(), char2.getLength());
     }

    /**
     * Compare two Strings using standard SQL semantics.
     *
     * @param op1               The first String
     * @param op2               The second String
     *
     * @return  -1 - op1 <  op2
     *           0 - op1 == op2
     *           1 - op1 > op2
     */
    protected static int stringCompare(
    char[]  op1,
    int     leftlen,
    char[]  op2,
    int     rightlen)
    {
        int         posn;
        char        leftchar;
        char        rightchar;
        int         retvalIfLTSpace;
        char[]      remainingString;
        int         remainingLen;

        /*
        ** By convention, nulls sort High, and null == null
        */
        if (op1 == null || op2 == null)
        {
            if (op1 != null)    // op2 == null
                return -1;
            if (op2 != null)    // op1 == null
                return 1;
            return 0;           // both null
        }
        /*
        ** Compare characters until we find one that isn't equal, or until
        ** one String or the other runs out of characters.
        */
        int shorterLen = leftlen < rightlen ? leftlen : rightlen;
        for (posn = 0; posn < shorterLen; posn++)
        {
            leftchar = op1[posn];
            rightchar = op2[posn];
            if (leftchar != rightchar)
            {
                if (leftchar < rightchar)
                    return -1;
                else
                    return 1;
            }
        }

        /*
        ** All the characters are equal up to the length of the shorter
        ** string.  If the two strings are of equal length, the values are
        ** equal.
        */
        if (leftlen == rightlen)
            return 0;

        /*
        ** One string is shorter than the other.  Compare the remaining
        ** characters in the longer string to spaces (the SQL standard says
        ** that in this case the comparison is as if the shorter string is
        ** padded with blanks to the length of the longer string.
        */
        if (leftlen > rightlen)
        {
            /*
            ** Remaining characters are on the left.
            */

            /* If a remaining character is less than a space,
             * return -1 (op1 < op2) */
            retvalIfLTSpace = -1;
            remainingString = op1;
            posn = rightlen;
            remainingLen = leftlen;
        }
        else
        {
            /*
            ** Remaining characters are on the right.
            */

            /* If a remaining character is less than a space,
             * return 1 (op1 > op2) */
            retvalIfLTSpace = 1;
            remainingString = op2;
            posn = leftlen;
            remainingLen = rightlen;
        }

        /* Look at the remaining characters in the longer string */
        for ( ; posn < remainingLen; posn++)
        {
            char    remainingChar;

            /*
            ** Compare the characters to spaces, and return the appropriate
            ** value, depending on which is the longer string.
            */

            remainingChar = remainingString[posn];

            if (remainingChar < ' ')
                return retvalIfLTSpace;
            else if (remainingChar > ' ')
                return -retvalIfLTSpace;
        }

        /* The remaining characters in the longer string were all spaces,
        ** so the strings are equal.
        */
        return 0;
    }

    /**
     * This method gets called for the collation sensitive char classes ie
     * CollatorSQLChar, CollatorSQLVarchar, CollatorSQLLongvarchar,
     * CollatorSQLClob. These collation sensitive chars need to have the
     * collation key in order to do string comparison. And the collation key
     * is obtained using the Collator object that these classes already have.
     *
     * @return CollationKey obtained using Collator on the string
     * @throws StandardException
     */
    protected CollationKey getCollationKey() throws StandardException
    {
        char tmpCharArray[];

        if (cKey != null)
            return cKey;

        if (rawLength == -1)
        {
            /* materialize the string if input is a stream */
            tmpCharArray = getCharArray();
            if (tmpCharArray == null)
                return null;
        }

        int lastNonspaceChar = rawLength;

        while (lastNonspaceChar > 0 &&
               rawData[lastNonspaceChar - 1] == '\u0020')
            lastNonspaceChar--;         // count off the trailing spaces.

        RuleBasedCollator rbc = getCollatorForCollation();
        cKey = rbc.getCollationKey(new String(rawData, 0, lastNonspaceChar));

        return cKey;
    }

    /*
     * String display of value
     */

    public String toString()
    {
        if (isNull()) {
            return "NULL";
        }

        if ((value == null) && (rawLength != -1)) {

            return new String(rawData, 0, rawLength);
        }

        if (stream != null) {
            try {
                return getString();
            } catch (Exception e) {
                return e.toString();
            }
        }

        return value;
    }

    /*
     * Hash code
     */
    public int hashCode()
    {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(!(this instanceof CollationElementsInterface),
                    "SQLChar.hashCode() does not work with collation");
        }

        try {
            if (getString() == null)
            {
                return 0;
            }
        }
        catch (StandardException se)
        {
            throw new RuntimeException(se);
        }


        /* value.hashCode() doesn't work because of the SQL blank padding
         * behavior.
         * We want the hash code to be based on the value after the
         * trailing blanks have been trimmed.  Calling trim() is too expensive
         * since it will create a new object, so here's what we do:
         *      o  Walk from the right until we've found the 1st
         *         non-blank character.
         *      o  Calculate the hash code based on the characters from the
         *         start up to the first non-blank character from the right.
         */

        // value will have been set by the getString() above
        String lvalue = value;

        // Find 1st non-blank from the right
        int lastNonPadChar = lvalue.length() - 1;
        while (lastNonPadChar >= 0 && lvalue.charAt(lastNonPadChar) == PAD) {
            lastNonPadChar--;
        }

        // Build the hash code. It should be identical to what we get from
        // lvalue.substring(0, lastNonPadChar+1).hashCode(), but it should be
        // cheaper this way since we don't allocate a new string.
        int hashcode = 0;
        for (int i = 0; i <= lastNonPadChar; i++) {
            hashcode = hashcode * 31 + lvalue.charAt(i);
        }

        return hashcode;
    }

    /**
     * Hash code implementation for collator sensitive subclasses.
     */
    int hashCodeForCollation() {
        CollationKey key = null;

        try {
            key = getCollationKey();
        } catch (StandardException se) {
            // ignore exceptions, like we do in hashCode()
            if (SanityManager.DEBUG) {
                SanityManager.THROWASSERT("Unexpected exception", se);
            }
        }

        return key == null ? 0 : key.hashCode();
    }

    /**
     * Get a SQLVarchar for a built-in string function.
     *
     * @return a SQLVarchar.
     *
     * @exception StandardException     Thrown on error
     */
    protected StringDataValue getNewVarchar() throws StandardException
    {
        return new SQLVarchar();
    }

    protected void setLocaleFinder(LocaleFinder localeFinder)
    {
        this.localeFinder = localeFinder;
    }

    /** @exception StandardException        Thrown on error */
    private Locale getLocale() throws StandardException
    {
        LocaleFinder finder = getLocaleFinder();
        if(finder==null) return Locale.getDefault();
        else return finder.getCurrentLocale();
    }

    protected RuleBasedCollator getCollatorForCollation()
        throws StandardException
    {
        if (SanityManager.DEBUG) {
            // Sub-classes that support collation will override this method,
            // do don't expect it to be called here in the base class.
            SanityManager.THROWASSERT("No support for collators in base class");
        }
        return null;
    }

    protected LocaleFinder getLocaleFinder()
    {
        // This is not very satisfactory, as it creates a dependency on
        // the DatabaseContext. It's the best I could do on short notice,
        // though.  -  Jeff
        if (localeFinder == null)
        {
            DatabaseContext dc = (DatabaseContext)
                ContextService.getContext(DatabaseContext.CONTEXT_ID);
            if( dc != null)
                localeFinder = dc.getDatabase();
        }

        return localeFinder;
    }

    public int estimateMemoryUsage()
    {
        int sz = BASE_MEMORY_USAGE + ClassSize.estimateMemoryUsage( value);
        if( null != rawData)
            sz += 2*rawData.length;
        // Assume that cKey, stream, and localFinder are shared,
        // so do not count their memory usage
        return sz;

    } // end of estimateMemoryUsage

    public void copyState(SQLChar other)
    {
        copyState
            (
             other.value,
             other.rawData,
             other.rawLength,
             other.cKey,
             other.stream,
             other._clobValue,
             other.localeFinder
             );
    }
    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "DB-9406")
    public void    copyState
        (
         String otherValue,
         char[] otherRawData,
         int otherRawLength,
         CollationKey otherCKey,
         InputStream    otherStream,
         Clob otherClobValue,
         LocaleFinder otherLocaleFinder
         )
    {
        value = otherValue;
        rawData = otherRawData;
        rawLength = otherRawLength;
        cKey = otherCKey;
        stream = otherStream;
        _clobValue = otherClobValue;
        localeFinder = otherLocaleFinder;
        isNull = evaluateNull();
    }

    /**
     * Gets a trace representation for debugging.
     *
     * @return a trace representation of this SQL Type.
     */
    public String getTraceString() throws StandardException {
        // Check if the value is SQL NULL.
        if (isNull()) {
            return "NULL";
        }

        return (toString());
    }

    /**
     * Returns the default stream header generator for the string data types.
     *
     * @return A stream header generator.
     */
    public StreamHeaderGenerator getStreamHeaderGenerator() {
        return CHAR_HEADER_GENERATOR;
    }

    /**
     * Sets the mode for the database being accessed.
     *
     * @param inSoftUpgradeMode {@code true} if the database is being accessed
     *      in soft upgrade mode, {@code false} if not, and {@code null} if
     *      unknown
     */
    public void setStreamHeaderFormat(Boolean inSoftUpgradeMode) {
        // Ignore this for CHAR, VARCHAR and LONG VARCHAR.
    }

    private int getClobLength() throws StandardException
    {
        try {
            return rawGetClobLength();
        }
        catch (SQLException se) { throw StandardException.plainWrapException( se ); }
    }

    private int rawGetClobLength() throws SQLException
    {
        long   maxLength = Integer.MAX_VALUE;
        long   length = _clobValue.length();
        if ( length > Integer.MAX_VALUE )
        {
            StandardException se = StandardException.newException
                ( SQLState.BLOB_TOO_LARGE_FOR_CLIENT, Long.toString( length ), Long.toString( maxLength ) );

            throw new SQLException( se.getMessage() );
        }

        return (int) length;
    }

    public Format getFormat() {
        return Format.CHAR;
    }

    public int getSqlCharSize() { return sqlCharSize; }
    public void setSqlCharSize(int size) { sqlCharSize = size; }

    public void read(Row row, int ordinal) throws StandardException {
        if (row.isNullAt(ordinal))
            setToNull();
        else {
            isNull = false;
            value = row.getString(ordinal);
        }
    }

    @Override
    public StructField getStructField(String columnName) {
        return DataTypes.createStructField(columnName, DataTypes.StringType, true);
    }


    public void updateThetaSketch(UpdateSketch updateSketch) {
        updateSketch.update(value);
    }

    @Override
    public void setSparkObject(Object sparkObject) throws StandardException {
        if (sparkObject == null)
            setToNull();
        else {
            value = sparkObject.toString();
            setIsNull(false);
        }
    }

}
