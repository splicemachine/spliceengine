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

package com.splicemachine.db.iapi.types;

/**
 * 
 * An alternative to java.lang.Double.parseDouble which isn't synchronised like
 *  the Java implementation is.
 *
 * Explanation in http://dalelane.co.uk/blog/?p=2936 
 *
 * @author Dale Lane (dale.lane@gmail.com)
 */
public class DoubleParser {

    //
    // String representations of special case numbers documented at 
    //  http://docs.oracle.com/javase/7/docs/api/java/lang/Double.html#toString(double) 
    // 
    
    private static final String INFINITY    = "Infinity";
    private static final String NEGINFINITY = "-Infinity"; 
    private static final String NAN         = "NaN";


    public static double parseDouble(String str) 
    {
        //
        // special case handling
        //

        if (str.equals(INFINITY)){
            return Double.POSITIVE_INFINITY;
    	}
        if (str.equals(NEGINFINITY)){
            return Double.NEGATIVE_INFINITY;
    	}
    	if (str.equals(NAN)){
            return Double.NaN;
    	}    


        int exp = 0;

        //
        // cleanup 
        // 

        str = stripUnnecessaryPlus(str);

        //
        // handle exponents included in the number
        //

        int expStrIdx = getExponentIdx(str);
        if (expStrIdx >= 0) {
            String expStr = stripUnnecessaryPlus(str.substring(expStrIdx + 1));
            exp = Short.parseShort(expStr);

            if (exp > 100 || exp < -100){
                // too small/large for this modified implementation 
                //  to handle, so give up and fallback to built-in 
                //  implementation
                return Double.parseDouble(str);
            }

            str = str.substring(0, expStrIdx);
        }


        //
        // handle fractions
        //

        int decPointIdx = str.indexOf(".");
        int numDigits = str.length();

        if (decPointIdx >= 0) {
            // we found a decimal point - we want to parse a whole number, so we 
            //  move the decimal point to the end, and update the exponent to 
            //  cancel out the change
            //
            // e.g. 
            // instead of trying to parse 1234.5678
            //  it's easier to parse      12345678 with exp of -4
            //

            // update exponent
            exp -= (numDigits - decPointIdx - 1);  // if there is already an exponent - add the difference
            
            // convert to whole number
            str = str.substring(0, decPointIdx) + str.substring(decPointIdx + 1);
            
            // removing the decimal point character has shrunk the string by 1 character
            numDigits -= 1;
        }
            
        // use the length of the number to guess it's size - choose data type
        //  that can fit (integer for numbers under 9 characters long, long for
        //  numbers under 18 characters long)
            
        if (numDigits <= LEN_INT){
            return Integer.parseInt(str) * getExponentValue(exp);
        }
        else if (numDigits <= LEN_LONG){
            return Long.parseLong(str) * getExponentValue(exp);
        }
        else {
            // number is too long to be parsed using Long.parseLong
            //  so we only parse the most significant digits and drop the rest
            
            final String mostSignificantDigitsStr = str.substring(0, LEN_LONG);
            final int expToAdd = numDigits - LEN_LONG;
            return Long.parseLong(mostSignificantDigitsStr) * getExponentValue(exp + expToAdd);
        }
    }



    // To save time calling Math.pow, we calculate values between 
    //  Math.pow(10.0, -256) and Math.pow(10.0, 256) so that we 
    //  can reuse the results
    private final static int PRE_COMPUTED_EXP_RANGE = 256;
    private final static double[] POS_EXPS = new double[PRE_COMPUTED_EXP_RANGE];
    private final static double[] NEG_EXPS = new double[PRE_COMPUTED_EXP_RANGE];
    static {
    	for (int i=0; i < PRE_COMPUTED_EXP_RANGE; i++){
    		POS_EXPS[i] = Math.pow(10.0, i);
    		NEG_EXPS[i] = Math.pow(10.0, -i);
    	}
    } 

    // The longest number that we can reliably fit in an int is 9 digits long
    //  as the biggest int is 2147483647 which is 10 digits long. 
    // Some 10 digit numbers can fit in an int (e.g. 2000000000) but others can't (e.g. 3000000000)
    //  so only numbers with 9 or fewer digits can definitely be fit into an int
    private final static int LEN_INT  = 9;

    // The longest number that we can reliably fit in a long is 18 digits long
    //  as the biggest long is 9223372036854775807 which is 19 digits long. 
    // Some 10 digit numbers can fit in a long (e.g. 9000000000000000000) but others can't (e.g. 9999999999999999999)
    //  so only numbers with 18 or fewer digits can definitely be fit into a long
    private final static int LEN_LONG = 18;


    // Calculate the value of the specified exponent - reuse a precalculated value if possible
    private final static double getExponentValue(int exp){
    	if (exp > -PRE_COMPUTED_EXP_RANGE){
    		if (exp <= 0){
    			return NEG_EXPS[-exp];
    		}
    		else if (exp < PRE_COMPUTED_EXP_RANGE){
    			return POS_EXPS[exp];
    		}
    	}
    	return Math.pow(10.0, exp);
    }

    // A number can be prefixed with a "+" but this makes no difference, so we can remove it
    private final static String stripUnnecessaryPlus(String str){
        if (str.startsWith("+")){
            return str.substring(1);
        }
        return str;
    }
    
    // Returns the location of an 'e' or 'E' in the provided string
    private final static int getExponentIdx(String str){
        int expIdx = str.indexOf("E");
        if (expIdx < 0){
            expIdx = str.indexOf("e");
        }
        return expIdx;
    }
}