package com.splicemachine.derby.utils;

import org.joda.time.*;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;




/**
 * Implementation of standard Splice Date functions,
 * in particular those represented as system procedures
 * in the SYSFUN schema, such that they can be invoked
 * without including the schema prefix in SQL statements.
 */
public class SpliceDateFunctions {

	public static Date ADD_MONTHS(java.sql.Date source, int numOfMonths) {
		if (source == null) return source;
		DateTime dt = new DateTime(source);
		return new Date(dt.plusMonths(numOfMonths).getMillis());
	}
	/**
	 * Implements the TO_DATE() fuction.
	 * @throws ParseException 
	 * 
	 * 
	 */
	public static java.sql.Date TO_DATE(String source, String format) throws ParseException{
		if(source == null || format == null) return null;
		SimpleDateFormat fmt = new SimpleDateFormat(format.toLowerCase().replaceAll("m", "M"));
		return new Date(fmt.parse(source).getTime());
	}
	/**
	 * Implements the LAST_DAY function
	 *  
	 */
	public static Date LAST_DAY(java.sql.Date source) {
		if(source == null) {
			return source;
		}
		DateTime dt = new DateTime(source).dayOfMonth().withMaximumValue();
		return new java.sql.Date(dt.getMillis());
	}
	/**
	 * Implements the NEXT_DAY function
	 */
	public static Date NEXT_DAY(java.sql.Date source, String weekday){
		if(source==null||weekday==null) return source;
		try{
			DateTime dt = new DateTime(source);
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			map.put("sunday", 1);
			map.put("monday", 2);
			map.put("tuesday", 3);
			map.put("wednesday", 4);
			map.put("thursday", 5);
			map.put("friday", 6);
			map.put("saturday", 7);
			int increment = map.get(weekday.toLowerCase())-dt.getDayOfWeek()-1;
			if(increment>0){
				return new Date(dt.plusDays(increment).getMillis());
			}
			else if(increment==0){
				return new Date(dt.plusDays(7).getMillis());
			}
			else{
				return new Date(dt.plusDays(7+increment).getMillis());
			}

		}  catch (NullPointerException e){
			System.out.print("Day does not exist");
			return null;
		}
	}
	/**11
	 * Implements the MONTH_BETWEEN function
	 * if any of the input values are null, the function will return -1. Else, it will return an positive double.
	 */
	public static double MONTH_BETWEEN(java.sql.Date source1, java.sql.Date source2){
		if(source1 == null || source2 == null) return -1;
		DateTime dt1 = new DateTime(source1);
		DateTime dt2 = new DateTime(source2);
		int months = Months.monthsBetween(dt1,dt2).getMonths();
		return months;
	}

	/**
	 * Implements the to_char function
	 * 
	 */
	public static String TO_CHAR(java.sql.Date source, String format){
		if(source == null || format == null) return null;
		SimpleDateFormat fmt = new SimpleDateFormat(format.toLowerCase().replaceAll("m", "M"));
		return fmt.format(source);
	}
	/**
	 * Implements the trunc_date function
	 */
	public static Timestamp TRUNC_DATE(Timestamp source, String field){
		if(source == null || field == null) return null;
		DateTime dt = new DateTime(source);
		field = field.toLowerCase();
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		map.put("microseconds", 1);
		map.put("milliseconds", 2);
		map.put("second",3);
		map.put("minute", 4);
		map.put("hour", 5);
		map.put("day", 6);
		map.put("week", 7);
		map.put("month", 8);
		map.put("quarter", 9);
		map.put("year", 10);
		map.put("decade", 11);
		map.put("century", 12);
		map.put("millennium", 13);
		try {
			int index = map.get(field.toLowerCase());
			if (index == 1) {
				int nanos = source.getNanos();
				nanos = nanos - nanos % 1000;
				source.setNanos(nanos);
				return source;
			} else if (index == 2) {
				int nanos = source.getNanos();
				nanos = nanos - nanos % 1000000;
				source.setNanos(nanos);
				return source;
			} else if (index == 3) {
				source.setNanos(0);
				return source;

			} else if (index == 4) {
				DateTime modified = dt.minusSeconds(dt.getSecondOfMinute());
				Timestamp ret = new Timestamp(modified.getMillis());
				ret.setNanos(0);
				return ret;
			} else if (index == 5) {
				DateTime modified = dt.minusMinutes(dt.getMinuteOfHour())
						.minusSeconds(dt.getSecondOfMinute());
				Timestamp ret = new Timestamp(modified.getMillis());
				ret.setNanos(0);
				return ret;
			} else if (index == 6) {
				DateTime modified = dt.minusHours(dt.getHourOfDay())
						.minusMinutes(dt.getMinuteOfHour())
						.minusSeconds(dt.getSecondOfMinute());
				Timestamp ret = new Timestamp(modified.getMillis());
				ret.setNanos(0);
				return ret;
			} else if (index == 7) {
				DateTime modified = dt.minusDays(dt.getDayOfWeek())
						.minusHours(dt.getHourOfDay())
						.minusMinutes(dt.getMinuteOfHour())
						.minusSeconds(dt.getSecondOfMinute());
				Timestamp ret = new Timestamp(modified.getMillis());
				ret.setNanos(0);
				return ret;
			} else if (index == 8) {
				DateTime modified = dt.minusDays(dt.get(DateTimeFieldType.dayOfMonth()) - 1)
						.minusHours(dt.getHourOfDay())
						.minusMinutes(dt.getMinuteOfHour())
						.minusSeconds(dt.getSecondOfMinute());
				Timestamp ret = new Timestamp(modified.getMillis());
				ret.setNanos(0);
				return ret;
			} else if (index == 9) {
				int month = dt.getMonthOfYear();
				DateTime modified = dt;
				if ((month + 1) % 3 == 1) {
					modified = dt.minusMonths(2);
				} else if ((month + 1) % 3 == 0) {
					modified = dt.minusMonths(1);
				}
				DateTime fin = modified.minusDays(dt.get(DateTimeFieldType.dayOfMonth()) - 1)
						.minusHours(dt.getHourOfDay())
						.minusMinutes(dt.getMinuteOfHour())
						.minusSeconds(dt.getSecondOfMinute());
				Timestamp ret = new Timestamp(fin.getMillis());
				ret.setNanos(0);
				return ret;
			} else if (index == 10) {
				DateTime modified = dt.minusDays(dt.get(DateTimeFieldType.dayOfMonth()) - 1)
						.minusHours(dt.getHourOfDay())
						.minusMonths(dt.getMonthOfYear() - 1)
						.minusMinutes(dt.getMinuteOfHour())
						.minusSeconds(dt.getSecondOfMinute());
				Timestamp ret = new Timestamp(modified.getMillis());
				ret.setNanos(0);
				return ret;
			} else if (index == 11) {
				DateTime modified = dt.minusDays(dt.get(DateTimeFieldType.dayOfMonth()) - 1)
						.minusYears(dt.getYear() % 10)
						.minusHours(dt.getHourOfDay())
						.minusMonths(dt.getMonthOfYear() - 1)
						.minusMinutes(dt.getMinuteOfHour())
						.minusSeconds(dt.getSecondOfMinute());
				Timestamp ret = new Timestamp(modified.getMillis());
				ret.setNanos(0);
				return ret;
			} else if (index == 12) {
				DateTime modified = dt.minusDays(dt.get(DateTimeFieldType.dayOfMonth()) - 1)
						.minusHours(dt.getHourOfDay())
						.minusYears(dt.getYear() % 100)
						.minusMonths(dt.getMonthOfYear() - 1)
						.minusMinutes(dt.getMinuteOfHour())
						.minusSeconds(dt.getSecondOfMinute());
				Timestamp ret = new Timestamp(modified.getMillis());
				ret.setNanos(0);
				return ret;
			} else {
				DateTime modified = dt.minusDays(dt.get(DateTimeFieldType.dayOfMonth()) - 1)
						.minusYears(dt.getYear() % 1000)
						.minusHours(dt.getHourOfDay())
						.minusMonths(dt.getMonthOfYear() - 1)
						.minusMinutes(dt.getMinuteOfHour())
						.minusSeconds(dt.getSecondOfMinute());
				Timestamp ret = new Timestamp(modified.getMillis());
				ret.setNanos(0);
				return ret;
			}
		}catch (NullPointerException e){
			System.out.println("time period does not exist");
			return source;
		}
	}
}
