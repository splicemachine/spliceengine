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
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.cache.ClassSize;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.i18n.LocaleFinder;
import com.splicemachine.db.iapi.services.io.ArrayInputStream;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl.Format;
import com.splicemachine.db.iapi.util.StringUtil;
import com.yahoo.sketches.theta.UpdateSketch;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.LocalDate;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * This contains an instance of a SQL Date.
 * <p>
 * The date is stored as int (year << 16 + month << 8 + day)
 * Null is represented by an encodedDate value of 0.
 * Some of the static methods in this class are also used by SQLTime and SQLTimestamp
 * so check those classes if you change the date encoding
 *
 * PERFORMANCE OPTIMIZATION:
 * The java.sql.Date object is only instantiated when needed
 * do to the overhead of Date.valueOf(), etc. methods.
 */

public final class SQLDate extends DataType
						implements DateTimeDataValue
{

	/**
	 +     * The JodaTime has problems with all the years before 1884
	 +     */

	public static final ThreadLocal<GregorianCalendar> GREGORIAN_CALENDAR =
			new ThreadLocal<GregorianCalendar>() {
				@Override
				protected GregorianCalendar initialValue() {
					return new GregorianCalendar();
				}
			};

	private int	encodedDate;	//year << 16 + month << 8 + day

    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog( SQLDate.class);

	private static boolean skipDBContext = false;

	public static void setSkipDBContext(boolean value) { skipDBContext = value; }

    public int estimateMemoryUsage()
    {
        return BASE_MEMORY_USAGE;
    } // end of estimateMemoryUsage

    public int getEncodedDate()
    {
        return encodedDate;
    }

	// Try to make modern dates close to the year 2020 take up less space.
	// Subtracting this value from a disk-encoded date allows us to use
	// 1 or 2 bytes for dates around year 2020 plus or minus 15 years,
	// vs. 3 or 4 bytes.
	public static final int DATE_ENCODING_OFFSET = 0xFC800;

	public int getDiskEncodedDate()
	{
		return (getYear(encodedDate)  << 9)  +
		       (getMonth(encodedDate) << 5)  +
		        getDay(encodedDate)          -
		       DATE_ENCODING_OFFSET;
	}


	/*
	** DataValueDescriptor interface
	** (mostly implemented in DataType)
	*/

	public String getString()
	{
		//format is [yyy]y-mm-dd e.g. 1-01-01, 9999-99-99
		if (!isNull())
		{
			return encodedDateToString(encodedDate);
		}
		else
		{
			return null;
		}
	}

	/**
		getTimestamp returns a timestamp with the date value 
		time is set to 00:00:00.0
	*/
	public Timestamp getTimestamp( Calendar cal) 
	{
		if (isNull())
		{
			return null;
		}
        
        return new Timestamp(getTimeInMillis(cal));
    }


	/**
	 * The JodaTime has problems with all the years before 1884
	 */
	public static final long JODA_CRUSH_YEAR = 1884;


    /**
     * Convert the date into a milli-seconds since the epoch
     * with the time set to 00:00 based upon the passed in Calendar.
     */
	private long getTimeInMillis(Calendar cal) {
		if (cal == null){
			cal = GREGORIAN_CALENDAR.get();
		}
		cal.clear();
		SQLDate.setDateInCalendar(cal,encodedDate);
		int year = getYear(encodedDate);
		cal.set(year,getMonth(encodedDate)-1,getDay(encodedDate));
		return cal.getTimeInMillis();
	}
    
    /**
     * Set the date portion of a date-time value into
     * the passed in Calendar object from its encodedDate
     * value. Only the YEAR, MONTH and DAY_OF_MONTH
     * fields are modified. The remaining
     * state of the Calendar is not modified.
     */
    static void setDateInCalendar(Calendar cal, int encodedDate)
    {
        // Note Calendar uses 0 for January, Derby uses 1.
        cal.set(getYear(encodedDate),
                getMonth(encodedDate)-1, getDay(encodedDate));     
    }
    
	/**
		getObject returns the date value

	 */
	public Object getObject()
	{
		return getDate( (Calendar) null);
	}
		
	public int getLength()
	{
		return 4;
	}

	/* this is for DataType's error generator */
	public String getTypeName()
	{
		return "DATE";
	}

	/*
	 * Storable interface, implies Externalizable, TypedFormat
	 */

	/**
		Return my format identifier.

		@see com.splicemachine.db.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() {
		return StoredFormatIds.SQL_DATE_ID;
	}

	/** 
		@exception IOException error writing data

	*/
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeBoolean(isNull);
		out.writeInt(encodedDate);
	}

	/**
	 * @see java.io.Externalizable#readExternal
	 *
	 * @exception IOException	Thrown on error reading the object
	 */
	public void readExternal(ObjectInput in) throws IOException {
		isNull = in.readBoolean();
		setValue(in.readInt());
	}

	public void readExternalFromArray(ArrayInputStream in) throws IOException
	{
		setValue(in.readInt());
	}

	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#cloneValue */
	public DataValueDescriptor cloneValue(boolean forceMaterialization)
	{
		// Call constructor with all of our info
		SQLDate local = null;
		try {
			local = new SQLDate(encodedDate);
		} catch (StandardException se) {
			throw new RuntimeException(se);
		}
		return local;
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
	public DataValueDescriptor getNewNull()
	{
		return new SQLDate();
	}
	/**
	 * @see com.splicemachine.db.iapi.services.io.Storable#restoreToNull
	 *
	 */

	public void restoreToNull()
	{
		// clear encodedDate
		encodedDate = 0;
		isNull = true;

	}

	/*
	 * DataValueDescriptor interface
	 */

	/** 
	 * @see DataValueDescriptor#setValueFromResultSet 
	 *
	 * @exception SQLException		Thrown on error
	 */
	public void setValueFromResultSet(ResultSet resultSet, int colNumber,
									  boolean isNullable)
		throws SQLException, StandardException
	{
        setValue(resultSet.getDate(colNumber), (Calendar) null);
	}

	/**
	 * Orderable interface
	 *
	 *
	 * @see com.splicemachine.db.iapi.types.Orderable
	 *
	 * @exception StandardException thrown on failure
	 */
	public int compare(DataValueDescriptor other)
		throws StandardException
	{
		/* Use compare method from dominant type, negating result
		 * to reflect flipping of sides.
		 */
		if (typePrecedence() < other.typePrecedence())
		{
			return - (other.compare(this));
		}


		boolean thisNull, otherNull;

		thisNull = this.isNull();
		otherNull = other.isNull();

		/*
		 * thisNull otherNull	return
		 *	T		T		 	0	(this == other)
		 *	F		T		 	-1 	(this < other)
		 *	T		F		 	1	(this > other)
		 */
		if (thisNull || otherNull)
		{
			if (!thisNull)		// otherNull must be true
				return -1;
			if (!otherNull)		// thisNull must be true
				return 1;
			return 0;
		}

		/*
			Neither are null compare them 
		 */

		int comparison;
		/* get the comparison date values */
		int otherVal = 0;

		/* if the argument is another SQLDate
		 * get the encodedDate
		 */
		if (other instanceof SQLDate)
		{
			otherVal = ((SQLDate)other).encodedDate; 
		}
		else 
		{
			/* O.K. have to do it the hard way and calculate the numeric value
			 * from the value
			 */
			otherVal = SQLDate.computeEncodedDate(other.getDate(GREGORIAN_CALENDAR.get()));
		}
		if (encodedDate > otherVal)
			comparison = 1;
		else if (encodedDate < otherVal)
			comparison = -1;
		else 
			comparison = 0;

		return comparison;
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
		if (!orderedNulls)		// nulls are unordered
		{
			if (this.isNull() || other.isNull())
				return unknownRV;
		}

		/* Do the comparison */
		return super.compare(op, other, orderedNulls, unknownRV);
	}

	/*
	** Class interface
	*/

	/*
	** Constructors
	*/

	/** no-arg constructor required by Formattable */
	public SQLDate() {
	}

	public SQLDate(Date value) throws StandardException
	{
		parseDate(value);
	}
    
    private void parseDate( java.util.Date value) throws StandardException
	{
		setValue(computeEncodedDate(value));
	}

	public SQLDate(int encodedDate) throws StandardException
	{
		setValue(encodedDate);
	}

    /**
     * Construct a date from a string. The allowed date formats are:
     *<ol>
     *<li>ISO: yyyy-mm-dd
     *<li>IBM USA standard: mm/dd/yyyy
     *<li>IBM European standard: dd.mm.yyyy
     *</ol>
     * Trailing blanks may be included; leading zeros may be omitted from the month and day portions.
     *
     * @param dateStr
     * @param isJdbcEscape if true then only the JDBC date escape syntax is allowed
     * @param localeFinder
     *
     * @exception StandardException if the syntax is invalid or the value is out of range.
     */
    public SQLDate( String dateStr, boolean isJdbcEscape, LocaleFinder localeFinder)
        throws StandardException
    {
        parseDate( dateStr, isJdbcEscape, localeFinder, (Calendar) null);
    }

    /**
     * Construct a date from a string. The allowed date formats are:
     *<ol>
     *<li>ISO: yyyy-mm-dd
     *<li>IBM USA standard: mm/dd/yyyy
     *<li>IBM European standard: dd.mm.yyyy
     *</ol>
     * Trailing blanks may be included; leading zeros may be omitted from the month and day portions.
     *
     * @param dateStr
     * @param isJdbcEscape if true then only the JDBC date escape syntax is allowed
     * @param localeFinder
     *
     * @exception StandardException if the syntax is invalid or the value is out of range.
     */
    public SQLDate( String dateStr, boolean isJdbcEscape, LocaleFinder localeFinder, Calendar cal)
        throws StandardException
    {
        parseDate( dateStr, isJdbcEscape, localeFinder, cal);
    }

    static final char ISO_SEPARATOR = '-';
    private static final char[] ISO_SEPARATOR_ONLY = {ISO_SEPARATOR};
    private static final char IBM_USA_SEPARATOR = '/';
    private static final char[] IBM_USA_SEPARATOR_ONLY = {IBM_USA_SEPARATOR};
    private static final char IBM_EUR_SEPARATOR = '.';
    private static final char[] IBM_EUR_SEPARATOR_ONLY = {IBM_EUR_SEPARATOR};
    private static final char[] END_OF_STRING = {(char) 0};
    
    private void parseDate( String dateStr, boolean isJdbcEscape, LocaleFinder localeFinder, Calendar cal)
        throws StandardException
    {
        boolean validSyntax = true;
        DateTimeParser parser = new DateTimeParser( dateStr);
        int year = 0;
        int month = 0;
        int day = 0;
        StandardException thrownSE = null;

        try
        {
            switch( parser.nextSeparator())
            {
            case ISO_SEPARATOR:
                setValue(SQLTimestamp.parseDateOrTimestamp( parser, false)[0]);
                return;

            case IBM_USA_SEPARATOR:
                if( isJdbcEscape)
                {
                    validSyntax = false;
                    break;
                }
                month = parser.parseInt( 2, true, IBM_USA_SEPARATOR_ONLY, false);
                day = parser.parseInt( 2, true, IBM_USA_SEPARATOR_ONLY, false);
                year = parser.parseInt( 4, false, END_OF_STRING, false);
                break;

            case IBM_EUR_SEPARATOR:
                if( isJdbcEscape)
                {
                    validSyntax = false;
                    break;
                }
                day = parser.parseInt( 2, true, IBM_EUR_SEPARATOR_ONLY, false);
                month = parser.parseInt( 2, true, IBM_EUR_SEPARATOR_ONLY, false);
                year = parser.parseInt( 4, false, END_OF_STRING, false);
                break;

            default:
                validSyntax = false;
            }
        }
        catch( StandardException se)
        {
            validSyntax = false;
            thrownSE = se;
        }
        if( validSyntax)
        {
            setValue(computeEncodedDate(year, month, day));
        }
        else
        {
            // See if it is a localized date or timestamp.
            dateStr = StringUtil.trimTrailing( dateStr);
            DateFormat dateFormat = null;
            if( localeFinder == null)
                dateFormat = DateFormat.getDateInstance();
            else if( cal == null)
                dateFormat = localeFinder.getDateFormat();
            else
                dateFormat = (DateFormat) localeFinder.getDateFormat().clone();
            if( cal != null)
                dateFormat.setCalendar( cal);
            try
            {
                setValue(computeEncodedDate(dateFormat.parse(dateStr), cal));
            }
            catch( ParseException pe)
            {
                // Maybe it is a localized timestamp
                try
                {
                    setValue(SQLTimestamp.parseLocalTimestamp( dateStr, localeFinder, cal)[0]);
                }
                catch( ParseException pe2)
                {
                    if( thrownSE != null)
                        throw thrownSE;
                    throw StandardException.newException( SQLState.LANG_DATE_SYNTAX_EXCEPTION);
                }
            }
        }
    } // end of parseDate

	/**
	 * Set the value from a correctly typed Date object.
	 * @throws StandardException 
	 */
	void setObject(Object theValue) throws StandardException
	{
		setValue((Date) theValue);
	}

	protected void setFrom(DataValueDescriptor theValue) throws StandardException {
		// Same format means same type SQLDate
		if (theValue instanceof SQLDate) {
			restoreToNull();
			setValue(((SQLDate) theValue).encodedDate);
		}
        else
        {
			//setValue(theValue.getDateTime());  // uses JodaTime which cannot handle old age
			setValue(theValue.getString());      // uses JodaTime for new age and Calendar for old age
        }
	}

	/**
		@see DateTimeDataValue#setValue

	 */
	public void setValue(Date value, Calendar cal)
		throws StandardException
	{
		restoreToNull();
		encodedDate = computeEncodedDate((java.util.Date) value, cal);
		isNull = evaluateNull();
	}

	/**
		@see DateTimeDataValue#setValue

	 */
	public void setValue(Timestamp value, Calendar cal)
		throws StandardException
	{
		restoreToNull();
		encodedDate = computeEncodedDate((java.util.Date) value, cal);
		isNull = evaluateNull();
	}
	
	public void setValue(DateTime value)
		throws StandardException
	{
		restoreToNull();		
		encodedDate = computeEncodedDate(value.getYear(),
						value.getMonthOfYear(),
						value.getDayOfMonth());
		isNull = evaluateNull();
	}

	public void setValue(int value)
	{
		encodedDate = value;
		isNull = evaluateNull();
	}


	public void setValue(String theValue) throws StandardException {
		setValue(theValue, null);
	}

	/*
	** SQL Operators
	*/

    NumberDataValue nullValueInt() {
        return new SQLInteger();
    }

    
	/**
	 * @see DateTimeDataValue#getYear
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getYear(NumberDataValue result)
        throws StandardException
	{
        if (isNull()) {
            return nullValueInt();
        } else {    
            return SQLDate.setSource(getYear(encodedDate), result);
        }
    }

	/**
	 * @see DateTimeDataValue#getQuarter
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getQuarter(NumberDataValue result)
							throws StandardException
	{
        if (isNull()) {
            return nullValueInt();
        } else {
            return SQLDate.setSource(getQuarter(encodedDate), result);
        }
	}

	/**
	 * @see DateTimeDataValue#getMonth
	 *
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getMonth(NumberDataValue result)
							throws StandardException
	{
        if (isNull()) {
            return nullValueInt();
        } else {
            return SQLDate.setSource(getMonth(encodedDate), result);
        }
	}

	/**
	 * @see DateTimeDataValue#getMonthName
	 *
	 * @exception StandardException		Thrown on error
	 */
	public StringDataValue getMonthName(StringDataValue result)
							throws StandardException {
        if (isNull()) {
            return new SQLVarchar();
        } else {
            return  SQLDate.setSource(getMonthName(encodedDate), result);
        }
	}

    /**
     * @see DateTimeDataValue#getWeek
     *
     * @exception StandardException		Thrown on error
     */
    public NumberDataValue getWeek(NumberDataValue result)
        throws StandardException {
        if (isNull()) {
            return nullValueInt();
        } else {
            return SQLDate.setSource(getWeek(encodedDate), result);
        }
    }

    /**
     * @see DateTimeDataValue#getWeekDay
     *
     * @exception StandardException		Thrown on error
     */
    public NumberDataValue getWeekDay(NumberDataValue result)
        throws StandardException {
        if (isNull()) {
            return nullValueInt();
        } else {
            return SQLDate.setSource(getWeekDay(encodedDate), result);
        }
    }

	/**
	 * @see DateTimeDataValue#getWeekDayName
	 *
	 * @exception StandardException		Thrown on error
	 */
	public StringDataValue getWeekDayName(StringDataValue result)
							throws StandardException {
        if (isNull()) {
            return new SQLVarchar();
        } else {
            return  SQLDate.setSource(getWeekDayName(encodedDate), result);
        }
	}

    /**
     * @see DateTimeDataValue#getDayOfYear
     *
     * @exception StandardException		Thrown on error
     */
    public NumberDataValue getDayOfYear(NumberDataValue result)
        throws StandardException {
        if (isNull()) {
            return nullValueInt();
        } else {
            return SQLDate.setSource(getDayOfYear(encodedDate), result);
        }
    }

	/**
	 * @see DateTimeDataValue#getDate
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getDate(NumberDataValue result)
							throws StandardException
	{
        if (isNull()) {
            return nullValueInt();
        } else {
            return SQLDate.setSource(getDay(encodedDate), result);
        }
	}

	/**
	 * @see DateTimeDataValue#getHours
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getHours(NumberDataValue result)
							throws StandardException
	{
		throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE, 
						"getHours", "Date");
	}

	/**
	 * @see DateTimeDataValue#getMinutes
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getMinutes(NumberDataValue result)
							throws StandardException
	{
		throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE, 
						"getMinutes", "Date");
	}

	/**
	 * @see DateTimeDataValue#getSeconds
	 * 
	 * @exception StandardException		Thrown on error
	 */
	public NumberDataValue getSeconds(NumberDataValue result)
							throws StandardException
	{
		throw StandardException.newException(SQLState.LANG_UNARY_FUNCTION_BAD_TYPE, 
						"getSeconds", "Date");
	}

	/*
	** String display of value
	*/

	public String toString()
	{
		if (isNull())
		{
			return "NULL";
		}
		else
		{
			return getDate( (Calendar) null).toString();
		}
	}

	/*
	 * Hash code
	 */
	public int hashCode()
	{
		return encodedDate;
	}

	/** @see DataValueDescriptor#typePrecedence */
	public int	typePrecedence()
	{
		return TypeId.DATE_PRECEDENCE;
	}

	/**
	 * Check if the value is null.  
	 * encodedDate is 0 if the value is null
	 *
	 * @return Whether or not value is logically null.
	 */
	private boolean evaluateNull()
	{
		return (encodedDate == 0);
	}

	/**
	 * Get the value field.  We instantiate the field
	 * on demand.
	 *
	 * @return	The value field.
	 */
	public Date getDate( Calendar cal)
	{
        if (isNull())
            return null;
		if (cal == null) {
			cal = GREGORIAN_CALENDAR.get();
		}
		cal.clear();
		SQLDate.setDateInCalendar(cal, encodedDate);
		return Date.valueOf(java.time.LocalDate.of(
				cal.get(Calendar.YEAR),
				cal.get(Calendar.MONTH)+1,
				cal.get(Calendar.DAY_OF_MONTH)));
	}
	
	
    /**
     * Get date from a SQLChar.
     *
     * @see DataValueDescriptor#getDate
     *
     * @exception StandardException thrown on failure to convert
     **/
    public DateTime getDateTime() throws StandardException {
    	return new DateTime(getTimeInMillis(null));
    }    

	/**
	 * Get the year from the encodedDate.
	 *
	 * @param encodedDate	the encoded date
	 * @return	 			year value.
	 */
	static int getYear(int encodedDate)
	{
		return (encodedDate >>> 16);
	}

	/**
	 * Get the quarter from the encodedDate,
     * January through March is one, April through June is two, etc.
	 *
	 * @param encodedDate	the encoded date
	 * @return	 			month value.
	 */
	static int getQuarter(int encodedDate)
	{
		return ((getMonth(encodedDate)-1)/3+1);
	}

	/**
	 * Get the month from the encodedDate,
     * January is one.
	 *
	 * @param encodedDate	the encoded date
	 * @return	 			month value.
	 */
	static int getMonth(int encodedDate)
	{
		return ((encodedDate >>> 8) & 0x00ff);
	}

	/**
	 * Get the month name from the encodedDate,
     * 'January' ,'February', etc.
	 *
	 * @param encodedDate	the encoded date
	 * @return	 			month name.
	 */
	static String getMonthName(int encodedDate) {
        LocalDate date = new LocalDate(getYear(encodedDate), getMonth(encodedDate), getDay(encodedDate));
        return date.monthOfYear().getAsText();
    }

	/**
	 * Get the week of year from the encodedDate,
     * 1-52.
	 *
	 * @param encodedDate	the encoded date
	 * @return	 			week day name.
	 */
	static int getWeek(int encodedDate) {
        LocalDate date = new LocalDate(getYear(encodedDate), getMonth(encodedDate), getDay(encodedDate));
        return date.weekOfWeekyear().get();
    }

	/**
	 * Get the day of week from the encodedDate,
     * 1-7.
	 *
	 * @param encodedDate	the encoded date
	 * @return	 			week day name.
	 */
	static int getWeekDay(int encodedDate) {
        LocalDate date = new LocalDate(getYear(encodedDate), getMonth(encodedDate), getDay(encodedDate));
        return date.dayOfWeek().get();
    }

	/**
	 * Get the week day name from the encodedDate,
     * 'Monday' ,'Tuesday', etc.
	 *
	 * @param encodedDate	the encoded date
	 * @return	 			week day name.
	 */
	static String getWeekDayName(int encodedDate) {
        LocalDate date = new LocalDate(getYear(encodedDate), getMonth(encodedDate), getDay(encodedDate));
        return date.dayOfWeek().getAsText();
    }

    /**
     * Get the day of year from the encodedDate,
     * 1-366.
     *
     * @param encodedDate	the encoded date
     * @return	 			week day name.
     */
    static int getDayOfYear(int encodedDate) {
        LocalDate date = new LocalDate(getYear(encodedDate), getMonth(encodedDate), getDay(encodedDate));
        return date.dayOfYear().get();
    }

	/**
	 * Get the day from the encodedDate.
	 *
	 * @param encodedDate	the encoded date
	 * @return	 			day value.
	 */
	static int getDay(int encodedDate)
	{
		return (encodedDate & 0x00ff);
	}
	/**
	 *	computeEncodedDate extracts the year, month and date from
	 *	a Calendar value and encodes them as
	 *		year << 16 + month << 8 + date
	 *	Use this function will help to remember to add 1 to month
	 *  which is 0 based in the Calendar class
	 *	@param cal	the Calendar 
	 *	@return 		the encodedDate
     *
     *  @exception StandardException if the value is out of the DB2 date range
	 */
	static int computeEncodedDate(Calendar cal) throws StandardException
	{
		if (cal.get(Calendar.ERA) == GregorianCalendar.BC)
			throw StandardException.newException( SQLState.LANG_DATE_RANGE_EXCEPTION);

		return computeEncodedDate(cal.get(Calendar.YEAR),
                                  cal.get(Calendar.MONTH) + 1,
                                  cal.get(Calendar.DATE));
	}

    static int computeEncodedDate( int y, int m, int d) throws StandardException
    {
        int maxDay = 31;
        switch( m)
        {
        case 4:
        case 6:
        case 9:
        case 11:
            maxDay = 30;
            break;
                
        case 2:
            // leap years are every 4 years except for century years not divisble by 400.
            maxDay = ((y % 4) == 0 && ((y % 100) != 0 || (y % 400) == 0)) ? 29 : 28;
            break;
        }
        if( y < 1 || y > 9999
            || m < 1 || m > 12
            || d < 1 || d > maxDay) {
            throw StandardException.newException( SQLState.LANG_DATE_RANGE_EXCEPTION);
        }
        return (y << 16) + (m << 8) + d;
    }

    /**
     * Convert a date to the JDBC representation and append it to a string buffer.
     *
     * @param year
     * @param month 1 based (January == 1)
     * @param day
     * @param sb The string representation is appended to this StringBuffer
     */
    static void dateToString( int year, int month, int day, StringBuffer sb)
    {
        String yearStr = Integer.toString( year);
        for( int i = yearStr.length(); i < 4; i++)
            sb.append( '0');
		sb.append(yearStr);
		sb.append(ISO_SEPARATOR);

		String monthStr = Integer.toString( month);
		String dayStr = Integer.toString( day);
		if (monthStr.length() == 1)
			sb.append('0');
		sb.append(monthStr);
		sb.append(ISO_SEPARATOR);
		if (dayStr.length() == 1)
			sb.append('0');
		sb.append(dayStr);
    } // end of dateToString
    
	/**
	 * Get the String version from the encodedDate.
	 *
	 * @return	 string value.
	 */
	static String encodedDateToString(int encodedDate)
	{
		StringBuffer vstr = new StringBuffer();
        dateToString( getYear(encodedDate), getMonth(encodedDate), getDay(encodedDate), vstr);
		return vstr.toString();
	}

	/**
		This helper routine tests the nullability of various parameters
		and sets up the result appropriately.

		If source is null, a new NumberDataValue is built. 

		@exception StandardException	Thrown on error
	 */
	static NumberDataValue setSource(int value,
										NumberDataValue source)
									throws StandardException {
		/*
		** NOTE: Most extract operations return int, so the generation of
		** a SQLInteger is here.  Those extract operations that return
		** something other than int must allocate the source NumberDataValue
		** themselves, so that we do not allocate a SQLInteger here.
		*/
		if (source == null)
			source = new SQLInteger();

		source.setValue(value);

		return source;
	}

	/**
		This helper routine tests the nullability of various parameters
		and sets up the result appropriately.

		If source is null, a new SQLVarchar is built.

		@exception StandardException	Thrown on error
	 */
	static StringDataValue setSource(String value,
                                StringDataValue source)
									throws StandardException {
		if (source == null)
			source = new SQLChar();

		source.setValue(value);

		return source;
	}

	/**
     * Compute the encoded date given a date
	 *
	 */
	public static int computeEncodedDate(java.util.Date value) throws StandardException
	{
        return computeEncodedDate( value, null);
    }

    static int computeEncodedDate(java.util.Date value, Calendar currentCal) throws StandardException
    {
		if (value == null)
			return 0;			//encoded dates have a 0 value for null
		
		int result;
		
        if( currentCal == null){
			currentCal=GREGORIAN_CALENDAR.get();
		}
		currentCal.setTime(value);
		result = SQLDate.computeEncodedDate(currentCal);

        return result;
	}
    
    static int computeEncodedDate(DateTime dateTime) throws StandardException
    {
    	return computeEncodedDate(dateTime.getYear(), dateTime.getMonthOfYear(), dateTime.getDayOfMonth());
    	
    }


        /**
         * Implement the date SQL function: construct a SQL date from a string, number, or timestamp.
         *
         * @param operand Must be a date or a string convertible to a date.
         * @param dvf the DataValueFactory
         *
         * @exception StandardException standard error policy
         */
    public static DateTimeDataValue computeDateFunction( DataValueDescriptor operand,
                                                         DataValueFactory dvf) throws StandardException
    {
        try
        {
            if( operand.isNull())
                return new SQLDate();
            if( operand instanceof SQLDate)
                return (SQLDate) operand.cloneValue(false);

            if( operand instanceof SQLTimestamp)
            {
                DateTimeDataValue retVal = new SQLDate();
                retVal.setValue( operand);
                return retVal;
            }
            if( operand instanceof NumberDataType)
            {
                int daysSinceEpoch = operand.getInt();
                if( daysSinceEpoch <= 0 || daysSinceEpoch > 3652059)
                    throw StandardException.newException( SQLState.LANG_INVALID_FUNCTION_ARGUMENT,
                                                          operand.getString(), "date");
                Calendar cal = new GregorianCalendar( 1970, 0, 1, 12, 0, 0);
				cal.add(Calendar.DATE, daysSinceEpoch - 1);
                return new SQLDate( computeEncodedDate( cal.get( Calendar.YEAR),
                                                        cal.get( Calendar.MONTH) + 1,
                                                        cal.get( Calendar.DATE)));
            }
            String str = operand.getString();
            if( str.length() == 7)
            {
                // yyyyddd where ddd is the day of the year
                int year = SQLTimestamp.parseDateTimeInteger( str, 0, 4);
                int dayOfYear = SQLTimestamp.parseDateTimeInteger( str, 4, 3);
                if( dayOfYear < 1 || dayOfYear > 366)
                    throw StandardException.newException( SQLState.LANG_INVALID_FUNCTION_ARGUMENT,
                                                          operand.getString(), "date");
                Calendar cal = new GregorianCalendar( year, 0, 1, 2, 0, 0);
                cal.add( Calendar.DAY_OF_YEAR, dayOfYear - 1);
                int y = cal.get( Calendar.YEAR);
                if( y != year)
                    throw StandardException.newException( SQLState.LANG_INVALID_FUNCTION_ARGUMENT,
                                                          operand.getString(), "date");
                return new SQLDate( computeEncodedDate( year,
                                                        cal.get( Calendar.MONTH) + 1,
                                                        cal.get( Calendar.DATE)));
            }
            // Else use the standard cast.
            return dvf.getDateValue( str, false);
        }
        catch( StandardException se)
        {
            if( SQLState.LANG_DATE_SYNTAX_EXCEPTION.startsWith( se.getSQLState()))
                throw StandardException.newException( SQLState.LANG_INVALID_FUNCTION_ARGUMENT,
                                                      operand.getString(), "date");
            throw se;
        }
    } // end of computeDateFunction

    /** Adding this method to ensure that super class' setInto method doesn't get called
      * that leads to the violation of JDBC spec( untyped nulls ) when batching is turned on.
      */     
    public void setInto(PreparedStatement ps, int position) throws SQLException, StandardException {

                  ps.setDate(position, getDate((Calendar) null));
     }


    /**
     * Add a number of intervals to a datetime value. Implements the JDBC escape TIMESTAMPADD function.
     *
     * @param intervalType One of FRAC_SECOND_INTERVAL, SECOND_INTERVAL, MINUTE_INTERVAL, HOUR_INTERVAL,
     *                     DAY_INTERVAL, WEEK_INTERVAL, MONTH_INTERVAL, QUARTER_INTERVAL, or YEAR_INTERVAL
     * @param intervalCount The number of intervals to add
     * @param currentDate Used to convert time to timestamp
     * @param resultHolder If non-null a DateTimeDataValue that can be used to hold the result. If null then
     *                     generate a new holder
     *
     * @return startTime + intervalCount intervals, as a timestamp
     *
     * @exception StandardException
     */
    public DateTimeDataValue timestampAdd( int intervalType,
                                           NumberDataValue intervalCount,
                                           java.sql.Date currentDate,
                                           DateTimeDataValue resultHolder)
        throws StandardException
    {
        return toTimestamp().timestampAdd( intervalType, intervalCount, currentDate, resultHolder);
    }

    private SQLTimestamp toTimestamp() throws StandardException
    {
        return new SQLTimestamp( getEncodedDate(), 0, 0);
    }
    
    /**
     * Finds the difference between two datetime values as a number of intervals. Implements the JDBC
     * TIMESTAMPDIFF escape function.
     *
     * @param intervalType One of FRAC_SECOND_INTERVAL, SECOND_INTERVAL, MINUTE_INTERVAL, HOUR_INTERVAL,
     *                     DAY_INTERVAL, WEEK_INTERVAL, MONTH_INTERVAL, QUARTER_INTERVAL, or YEAR_INTERVAL
     * @param time1
     * @param currentDate Used to convert time to timestamp
     * @param resultHolder If non-null a NumberDataValue that can be used to hold the result. If null then
     *                     generate a new holder
     *
     * @return the number of intervals by which this datetime is greater than time1
     *
     * @exception StandardException
     */
    public NumberDataValue timestampDiff( int intervalType,
                                          DateTimeDataValue time1,
                                          java.sql.Date currentDate,
                                          NumberDataValue resultHolder)
        throws StandardException
    {
        return toTimestamp().timestampDiff(intervalType, time1, currentDate, resultHolder);
    }

    @Override
    public DateTimeDataValue plus(DateTimeDataValue leftOperand, NumberDataValue daysToAdd, DateTimeDataValue returnValue) throws StandardException {
		if( returnValue == null)
			returnValue = new SQLDate();
		if( isNull() || daysToAdd.isNull())
		{
			returnValue.restoreToNull();
			return returnValue;
		}
		DateTime dateAdd = new DateTime(leftOperand.getDateTime()).plusDays(daysToAdd.getInt());
		returnValue.setValue(dateAdd);
		return returnValue;
	}

    @Override
    public DateTimeDataValue minus(DateTimeDataValue leftOperand, NumberDataValue daysToSubtract, DateTimeDataValue returnValue) throws StandardException {
		if( returnValue == null)
			returnValue = new SQLDate();
		if(leftOperand.isNull() || daysToSubtract.isNull()) {
			returnValue.restoreToNull();
			return returnValue;
		}
		DateTime diff = leftOperand.getDateTime().minusDays(daysToSubtract.getInt());
		returnValue.setValue(diff);
		return returnValue;
	}

    @Override
    public NumberDataValue minus(DateTimeDataValue leftOperand, DateTimeDataValue rightOperand, NumberDataValue returnValue) throws StandardException {
		if( returnValue == null)
			returnValue = new SQLInteger();
		if(leftOperand.isNull() || rightOperand.isNull()) {
			returnValue.restoreToNull();
			return returnValue;
		}
		DateTime thatDate = rightOperand.getDateTime();
		Days diff = Days.daysBetween(thatDate, leftOperand.getDateTime());
		returnValue.setValue(diff.getDays());
		return returnValue;
	}

	@Override
	public void setValue(String theValue,Calendar cal) throws StandardException{
		restoreToNull();

		if (theValue != null)
		{
			DatabaseContext databaseContext = (skipDBContext ? null : (DatabaseContext) ContextService.getContext(DatabaseContext.CONTEXT_ID));
			parseDate( theValue,
					false,
					(databaseContext == null) ? null : databaseContext.getDatabase(),
					cal);
		}
		isNull = evaluateNull();
	}

	public Format getFormat() {
    	return Format.DATE;
    }

	@Override
	public void read(Row row, int ordinal) throws StandardException {
		if (row.isNullAt(ordinal))
			setToNull();
		else {
			java.time.LocalDate localeDate = row.getDate(ordinal).toLocalDate();
			encodedDate = computeEncodedDate(localeDate.getYear(),localeDate.getMonthValue(),localeDate.getDayOfMonth());
			isNull = false;
		}
	}

	@Override
	public StructField getStructField(String columnName) {
		return DataTypes.createStructField(columnName, DataTypes.DateType, true);
	}


	public void updateThetaSketch(UpdateSketch updateSketch) {
		updateSketch.update(encodedDate);
	}

	@Override
	public void setSparkObject(Object sparkObject) throws StandardException {
		if (sparkObject == null)
			setToNull();
		else {
			java.time.LocalDate localeDate = ((Date)sparkObject).toLocalDate();
			encodedDate = computeEncodedDate(localeDate.getYear(),localeDate.getMonthValue(),localeDate.getDayOfMonth());
			setIsNull(false);
		}

	}

}
