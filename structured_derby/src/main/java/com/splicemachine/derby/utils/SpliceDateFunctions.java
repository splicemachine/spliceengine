package com.splicemachine.derby.utils;

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
		Calendar c = Calendar.getInstance();
	    c.setTime(source);
	    c.add(Calendar.MONTH, numOfMonths);
	    return new java.sql.Date(c.getTimeInMillis());
	}
	/**
	 * Implements the TO_DATE() fuction.
	 * @throws ParseException 
	 * 
	 * 
	 */
	public static Date TO_DATE(String source, String format) throws ParseException{
		if(source == null || format == null) return null;
		SimpleDateFormat fmt = new SimpleDateFormat(format.toLowerCase().replaceAll("m", "M"));
		java.util.Date inmd = fmt.parse(source);
		java.sql.Date ret = new Date(inmd.getTime());
		return ret;
	}
	/**
	 * Implements the LAST_DAY function
	 *  
	 */
	public static Date LAST_DAY(java.sql.Date source) {
		if(source == null) {
			return source;
		}
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(source);
		calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
		return new java.sql.Date(calendar.getTimeInMillis());
	}
	/**
	 * Implements the NEXT_DAY function
	 */
	public static Date NEXT_DAY(java.sql.Date source, String weekday){
		if(source==null||weekday==null) return source;
		try{
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(source);
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			map.put("sunday", 1);
			map.put("monday", 2);
			map.put("tuesday", 3);
			map.put("wednesday", 4);
			map.put("thursday", 5);
			map.put("friday", 6);
			map.put("saturday", 7);
			int increment = map.get(weekday.toLowerCase())-calendar.get(Calendar.DAY_OF_WEEK);
			if(increment>0){
				calendar.add(Calendar.DAY_OF_WEEK, increment);
			}
			else if(increment==0){
				calendar.add(Calendar.DAY_OF_WEEK, 7);
			}
			else{
				calendar.add(Calendar.DAY_OF_WEEK, 7+increment);
			}
			Date ret = new Date(calendar.getTime().getTime());
			return ret;
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
		Calendar cal1 = Calendar.getInstance();
		cal1.setTime(source1);
		Calendar cal2 = Calendar.getInstance();
		cal2.setTime(source2);
		double ret = 0;
		ret = (cal1.get(Calendar.YEAR)-cal2.get(Calendar.YEAR))*12+(cal1.get(Calendar.MONTH)-cal2.get(Calendar.MONTH));
		if(cal1.get(Calendar.DAY_OF_MONTH)!=cal1.getActualMaximum(Calendar.DAY_OF_MONTH)
				|| cal2.get(Calendar.DAY_OF_MONTH)!=cal2.getActualMaximum(Calendar.DAY_OF_MONTH)){
			ret += ((cal1.get(Calendar.DAY_OF_MONTH)-cal2.get(Calendar.DAY_OF_MONTH))/31d);
		}
		ret = Math.abs(ret);
		return ret;
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
        Calendar c = Calendar.getInstance();
        c.setTime(source);
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
        int index = map.get(field.toLowerCase());
        if(index == 1){
           int nanos = source.getNanos();
            nanos = nanos - nanos%1000;
            source.setNanos(nanos);
            return source;
        }
        else if(index == 2){
            int nanos = source.getNanos();
            nanos = nanos - nanos%1000000;
            source.setNanos(nanos);
            return source;
        }
        else if(index == 3){
            source.setNanos(0);
            return source;

        }
        else if (index == 4){
            c.set(Calendar.SECOND,0);
            c.set(Calendar.MILLISECOND,0);
            Timestamp ret = new Timestamp(c.getTimeInMillis());
            ret.setNanos(0);
            return ret;
        }
        else if(index == 5){
            c.set(Calendar.MINUTE,0);
            c.set(Calendar.SECOND,0);
            c.set(Calendar.MILLISECOND,0);
            Timestamp ret = new Timestamp(c.getTimeInMillis());
            ret.setNanos(0);
            return ret;
        }
        else if (index == 6){
            c.set(Calendar.HOUR_OF_DAY,0);
            c.set(Calendar.MINUTE,0);
            c.set(Calendar.SECOND,0);
            c.set(Calendar.MILLISECOND,0);
            Timestamp ret = new Timestamp(c.getTimeInMillis());
            ret.setNanos(0);
            return ret;
        }
        else if (index == 7){
            c.set(Calendar.DAY_OF_WEEK,0);
            c.set(Calendar.HOUR_OF_DAY,0);
            c.set(Calendar.MINUTE,0);
            c.set(Calendar.SECOND,0);
            c.set(Calendar.MILLISECOND,0);
            c.add(Calendar.WEEK_OF_YEAR,-1);
            c.add(Calendar.DAY_OF_YEAR,1);
            Timestamp ret = new Timestamp(c.getTimeInMillis());
            ret.setNanos(0);
            return ret;
        }
        else if (index == 8){
            c.set(Calendar.DAY_OF_MONTH,0);
            c.set(Calendar.HOUR_OF_DAY,0);
            c.set(Calendar.MINUTE,0);
            c.set(Calendar.SECOND,0);
            c.set(Calendar.MILLISECOND,0);
            c.add(Calendar.DAY_OF_YEAR,1);
            Timestamp ret = new Timestamp(c.getTimeInMillis());
            ret.setNanos(0);
            return ret;
        }
        else if (index == 9){
            int month = c.get(Calendar.MONTH);
            if((month+1)%3==2){
                c.set(Calendar.MONTH,month-1);
            }
            else if((month+1)%3==0){
                c.set(Calendar.MONTH,month-2);
            }
            c.set(Calendar.DAY_OF_MONTH,0);
            c.set(Calendar.HOUR_OF_DAY,0);
            c.set(Calendar.MINUTE,0);
            c.set(Calendar.SECOND,0);
            c.set(Calendar.MILLISECOND,0);
            c.add(Calendar.DAY_OF_YEAR,1);
            Timestamp ret = new Timestamp(c.getTimeInMillis());
            ret.setNanos(0);
            return ret;
        }
        else if (index == 10){
            c.set(Calendar.MONTH,0);
            c.set(Calendar.DAY_OF_MONTH,0);
            c.set(Calendar.HOUR_OF_DAY,0);
            c.set(Calendar.MINUTE,0);
            c.set(Calendar.SECOND,0);
            c.set(Calendar.MILLISECOND,0);
            c.add(Calendar.DAY_OF_YEAR,1);
            Timestamp ret = new Timestamp(c.getTimeInMillis());
            ret.setNanos(0);
            return ret;
        }
        else if (index == 11){
            int year = c.get(Calendar.YEAR);
            c.set(Calendar.YEAR,year-(year%10));
            c.set(Calendar.MONTH,0);
            c.set(Calendar.DAY_OF_MONTH,0);
            c.set(Calendar.HOUR_OF_DAY,0);
            c.set(Calendar.MINUTE,0);
            c.set(Calendar.SECOND,0);
            c.set(Calendar.MILLISECOND,0);
            c.add(Calendar.DAY_OF_YEAR,1);
            Timestamp ret = new Timestamp(c.getTimeInMillis());
            ret.setNanos(0);
            return ret;
        }
        else if (index == 12){
            int year = c.get(Calendar.YEAR);
            c.set(Calendar.YEAR,year-(year%100));
            c.set(Calendar.MONTH,0);
            c.set(Calendar.DAY_OF_MONTH,0);
            c.set(Calendar.HOUR_OF_DAY,0);
            c.set(Calendar.MINUTE,0);
            c.set(Calendar.SECOND,0);
            c.set(Calendar.MILLISECOND,0);
            c.add(Calendar.DAY_OF_YEAR,1);
            Timestamp ret = new Timestamp(c.getTimeInMillis());
            ret.setNanos(0);
            return ret;
        }
        else {
            int year = c.get(Calendar.YEAR);
            c.set(Calendar.YEAR,year-(year%1000));
            c.set(Calendar.MONTH,0);
            c.set(Calendar.DAY_OF_MONTH,0);
            c.set(Calendar.HOUR_OF_DAY,0);
            c.set(Calendar.MINUTE,0);
            c.set(Calendar.SECOND,0);
            c.set(Calendar.MILLISECOND,0);
            c.add(Calendar.DAY_OF_YEAR,1);
            Timestamp ret = new Timestamp(c.getTimeInMillis());
            ret.setNanos(0);
            return ret;
        }

    }
	
}
