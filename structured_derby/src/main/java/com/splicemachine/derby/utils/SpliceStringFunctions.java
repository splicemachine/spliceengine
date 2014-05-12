package com.splicemachine.derby.utils;

import org.apache.commons.lang.WordUtils;

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
}
