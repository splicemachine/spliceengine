/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.utils;

import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

import com.splicemachine.db.iapi.util.StringUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.text.WordUtils;

import splice.com.google.common.cache.CacheBuilder;
import splice.com.google.common.cache.CacheLoader;
import splice.com.google.common.cache.LoadingCache;

/**
 * Implementation of standard Splice String functions,
 * in particular those represented as system procedures
 * in the SYSFUN schema, such that they can be invoked
 * using the same SQL syntax as the true built-in functions
 * (e.g., SUBSTR, LOCATE, etc.), without specifying
 * the schema prefix.
 * 
 * @author Walt Koetke
 */

public class SpliceStringFunctions {

    /**
     * Implements logic for the SQL function INSTR.
     * 
     * @param srcStr the source string to be checked for its contents
     * @param subStr the substring to be found in the source string
     * 
     * @return position (as non zero integer) within the source string
     * where the substring was found, or 0 (zero) if not found.
     */
    public static int INSTR(String srcStr, String subStr)
    {
    	// Matches support in MySql these two arguments,
    	// although Oracle's has some additional arguments
    	// (starting position and occurrence count).
    	// We could add these if we add support for
    	// optional arguments.
    	
    	if (srcStr == null) return 0;
    	if (subStr == null || subStr.isEmpty()) return 0;
    	// Returns position starting from zero or -1 if not found
        int index = srcStr.indexOf(subStr);
        // Return position starting from 1 or 0 if not found
        return index + 1;
    }

    /**
     * Implements logic for the SQL function INITCAP.
     * 
     * @param source the String to be capitalized
     * 
     * @return the capitalized String
     */
	public static String INITCAP(String source) {
		return WordUtils.capitalizeFully(source);
	}
	
    /**
     * Implements logic for the SQL function CONCAT.
     * 
     * @param arg1 first string
     * @param arg2 second string
     * 
     * @return concatenation of arg1 and arg2
     */
    public static String CONCAT(String arg1, String arg2)
    {
    	// Per MySql documentation, if any argument is NULL,
    	// function returns NULL.
    	if (arg1 == null || arg2 == null) {
    		return null;
    	}
    	return arg1 + arg2;
    }

    /**
     * Implements logic for the SQL function REGEXPLIKE.
     * 
     * @param s string to be evaluated
     * @param regexp regular expression
     *
     * @return flag indicating whether or not there is a match
     */
    public static boolean REGEXP_LIKE(String s, String regexp)
    {
    	try {

			return (s != null) ? patternCache.get(regexp).matcher(s).matches() : false;
	    } catch (ExecutionException e) {
	        throw new RuntimeException(String.format("Unable to fetch Pattern for regexp [%s]", regexp), e);
	    }
    }

    /**
     * Implements logic for the SQL function CHR.
     *
     * @param i INTEGER or SMALLINT to be evaluated
     * @return Returns the character that has the ASCII code value specified by the argument
     */
    public static String CHR(Integer i)
    {
        if (i == null)
            return null;
        if (i < 0)
            return String.valueOf((char)255);
        else if (i >= 256)
            return String.valueOf((char)(i % 256));
        else
            return String.valueOf((char)i.intValue());
    }

    /**
     * Implements logic for the SQL function HEX.
     *
     * @param s An expression that returns a value with a maximum length of 16 336 bytes.
     * @return Returns a hexadecimal representation of a value as a character string
     */
    @SuppressFBWarnings(value = "DM_DEFAULT_ENCODING", justification = "DB-9844")
    public static String HEX(String s)
    {
        if (s == null)
            return null;
        else
            return StringUtil.toHexString(s.getBytes(),0,s.length()).toUpperCase();

    }


    private static LoadingCache<String, Pattern> patternCache = CacheBuilder.newBuilder().maximumSize(100).build(
        new CacheLoader<String, Pattern>() {
            @Override
            public Pattern load(String regexp) {
                return Pattern.compile(regexp, Pattern.CASE_INSENSITIVE);
            }
        }
    );
}
