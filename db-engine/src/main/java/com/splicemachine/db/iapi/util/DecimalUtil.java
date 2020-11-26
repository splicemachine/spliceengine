/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.db.iapi.util;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl;
import com.splicemachine.db.iapi.types.SQLDecfloat;
import com.splicemachine.db.iapi.types.SQLDecimal;
import com.splicemachine.primitives.Bytes;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.text.DecimalFormatSymbols;
import java.util.Calendar;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public interface DecimalUtil {

    /**
     * Implements DB2 decimal function, for more information go to:
     * https://www.ibm.com/support/producthub/db2/docs/content/SSEPGG_11.5.0/com.ibm.db2.luw.sql.ref.doc/doc/r0000791.html
     */
    static DataValueDescriptor getDecimalDb2(DataValueDescriptor dvd,
                                             int precision,
                                             int scale,
                                             String decimalCharacter) throws StandardException {
        if (dvd == null || dvd.isNull()) {
            return null;
        }
        BigDecimal bigDecimal = null;
        Calendar cal = null;
        int roundingMode = BigDecimal.ROUND_DOWN;
        switch (DataValueFactoryImpl.Format.formatFor(dvd)) {
            case TINYINT: // fallthrough
            case SMALLINT: // fallthrough
            case INTEGER: // fallthrough
            case LONGINT: {
                if (String.valueOf(Math.abs(dvd.getLong())).length() > (precision - scale)) {
                    throw StandardException.newException(SQLState.LANG_INVALID_DECIMAL_CONVERSION, dvd.toString(), precision, scale); // DB2 SQLSTATE 22003
                }
                bigDecimal = new BigDecimal(BigInteger.valueOf(dvd.getLong()));
            }
            break;
            case DECIMAL: {
                bigDecimal = SQLDecimal.getBigDecimal(dvd);
                if (bigDecimal == null) return null;
            }
            break;
            case DECFLOAT: {
                bigDecimal = SQLDecfloat.getBigDecimal(dvd);
                roundingMode = BigDecimal.ROUND_HALF_EVEN; // should be configurable (DB-10672)
                if (bigDecimal == null) return null;
            }
            break;
            case CHAR: // fallthrough
            case VARCHAR: {
                String input = dvd.getString();
                if (input == null) return null;
                input = input.trim();
                if (input.isEmpty()) {
                    throw StandardException.newException(SQLState.LANG_INVALID_DECIMAL_STRING, dvd.toString(), precision, scale); // DB2 SQLSTATE 22018
                }
                if(decimalCharacter.length() > 1) {
                    throw StandardException.newException(SQLState.LANG_INVALID_DECIMAL_CHARACTER, decimalCharacter, precision, scale); // DB2 SQLSTATE 42815
                } else if(decimalCharacter.length() == 0) {
                    decimalCharacter = String.valueOf(new DecimalFormatSymbols().getDecimalSeparator());
                }
                boolean isNegative = false;
                if (input.charAt(0) == '-') {
                    isNegative = true;
                    input = input.substring(1);
                } else if (input.charAt(0) == '+') {
                    input = input.substring(1);
                }
                Pattern p = Pattern.compile("[^\\d]");
                Matcher m = p.matcher(input);
                int commaIndex = -1;
                if (m.find()) {
                    commaIndex = m.start();
                    if(commaIndex != -1 && (decimalCharacter.isEmpty() || input.charAt(commaIndex) != decimalCharacter.charAt(0))) {
                        throw StandardException.newException(SQLState.LANG_INVALID_DECIMAL_STRING, dvd.toString(), precision, scale); // DB2 SQLSTATE 22018
                    }
                    if (m.find()) { // another non-numeric character, bail out
                        throw StandardException.newException(SQLState.LANG_INVALID_DECIMAL_STRING, dvd.toString(), precision, scale); // DB2 SQLSTATE 22018
                    }
                }
                int stringScale = 0;
                if (commaIndex != -1) {
                    stringScale = input.length() - commaIndex - 1;
                }
                input = input.replaceAll("[^\\d]", ""); // remove comma if exists
                if (isNegative) {
                    input = "-" + input;
                }
                bigDecimal = new BigDecimal(new BigInteger(input), stringScale);
            }
            break;
            case DATE:
                bigDecimal = new BigDecimal(new BigInteger(dvd.getDate(cal).toString().replaceAll("[^\\d]", "")), 0);
                break;
            case TIME:
                bigDecimal = new BigDecimal(new BigInteger(dvd.getTime(cal).toString().replaceAll("[^\\d]", "")), 0);
                break;
            case TIMESTAMP:
                String timestamp = dvd.getTimestamp(cal).toString();
                int nanosScale = timestamp.indexOf('.');
                nanosScale = nanosScale == -1 ? 0 : timestamp.length() - nanosScale - 1;
                bigDecimal = new BigDecimal(new BigInteger(timestamp.replaceAll("[^\\d]", "")), nanosScale);
                break;
            case REAL:
                bigDecimal = BigDecimal.valueOf(dvd.getFloat());
                break;
            case DOUBLE:
                bigDecimal = BigDecimal.valueOf(dvd.getDouble());
                break;
            default:
                throw StandardException.newException(SQLState.LANG_INVALID_DECIMAL_TYPE, DataValueFactoryImpl.Format.formatFor(dvd).toString()); // DB2 SQLSTATE 42884
        }
        if ((bigDecimal.precision() - bigDecimal.scale()) > (precision - scale)) {
            throw StandardException.newException(SQLState.LANG_INVALID_DECIMAL_CONVERSION, dvd.toString(), precision, scale); // DB2 SQLSTATE 22003
        }
        bigDecimal = bigDecimal.setScale(scale, roundingMode);
        return new SQLDecimal(bigDecimal, precision, scale);
    }
}
