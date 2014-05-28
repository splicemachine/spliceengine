package com.splicemachine.derby.utils;

import java.sql.Date;
import java.util.Calendar;

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
}
