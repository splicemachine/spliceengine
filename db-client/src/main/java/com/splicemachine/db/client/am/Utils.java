/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.am;

import java.sql.SQLException;
import com.splicemachine.db.shared.common.reference.MessageId;

// Self-contained utilities.
// Don't reference any other driver classes, except Configuration, from within this class.

public final class Utils {

    /**
     * Quote an SQL identifier by enclosing it in double-quote characters
     * and escaping any double-quote characters with an extra double-quote
     * character.
     *
     * @param identifier the identifier to quote
     * @return the quoted identifier
     */
    static String quoteSqlIdentifier(String identifier) {
        // In the common case the length of the return value is the length of
        // the identifier plus the two surrounding double quotes. Use that as
        // the initial capacity of the buffer.
        StringBuilder retValue = new StringBuilder(identifier.length() + 2);

        final char quote = '"';

        retValue.append(quote);

        for (int i = 0; i < identifier.length(); i++) {
            char ch = identifier.charAt(i);
            if (ch == quote) {
                retValue.append(quote);
            }
            retValue.append(ch);
        }

        retValue.append(quote);

        return retValue.toString();
    }

    static String getStringFromBytes(byte[] bytes) {
        if (bytes == null) {
            return "{}";
        }
        StringBuilder stringBuffer = new StringBuilder(bytes.length * 6 + 4);
        stringBuffer.append("{ ");
        for (int i = 0; i < bytes.length; i++) {
            stringBuffer.append("0x");
            stringBuffer.append(Integer.toHexString(bytes[i] & 0xff));
            if (i != bytes.length - 1) {
                stringBuffer.append(", ");
            }
        }
        stringBuffer.append(" }");
        return stringBuffer.toString();
    }

    static String getStringFromInts(int[] ints) {
        if (ints == null) {
            return "{}";
        }
        StringBuilder stringBuffer = new StringBuilder();
        stringBuffer.append("{ ");
        for (int i = 0; i < ints.length; i++) {
            stringBuffer.append(String.valueOf(ints[i]));
            if (i != ints.length - 1) {
                stringBuffer.append(", ");
            }
        }
        stringBuffer.append(" }");
        return stringBuffer.toString();
    }

    static String getStringFromStrings(String[] strings) {
        if (strings == null) {
            return "{}";
        }
        StringBuilder stringBuffer = new StringBuilder();
        stringBuffer.append("{ ");
        for (int i = 0; i < strings.length; i++) {
            stringBuffer.append(strings[i]);
            if (i != strings.length - 1) {
                stringBuffer.append(", ");
            }
        }
        stringBuffer.append(" }");
        return stringBuffer.toString();
    }

    static public int computeBigDecimalPrecision(java.math.BigDecimal decimal) {
        byte[] bBytes = decimal.unscaledValue().abs().toByteArray();

        if (byteArrayCmp(bBytes, tenRadixArr[tenRadixArr.length - 1]) >= 0) {
            throw new java.lang.IllegalArgumentException(
                SqlException.getMessageUtil().
                    getTextMessage(MessageId.CONN_PRECISION_TOO_LARGE));
        }

        int lo = 0, hi = tenRadixArr.length - 1, mi = (hi + lo) / 2;
        do {
            int compare = byteArrayCmp(bBytes, tenRadixArr[mi]);
            if (compare == 1) {
                lo = mi;
            } else if (compare == -1) {
                hi = mi;
            } else {
                break;
            }

            mi = (hi + lo) / 2;
        } while (mi != lo);

        return (mi + 1);
    }

    // Used only by computeBigDecimalPrecision()
    private static int byteArrayCmp(byte[] arg1, byte[] arg2) {
        int arg1Offset = 0;
        int arg2Offset = 0;
        int length;
        if (arg1.length > arg2.length) {
            int diff = arg1.length - arg2.length;
            for (; arg1Offset < diff; arg1Offset++) {
                if (arg1[arg1Offset] != 0) {
                    return 1;
                }
            }
            length = arg2.length;
        } else if (arg1.length < arg2.length) {
            int diff = arg2.length - arg1.length;
            for (; arg2Offset < diff; arg2Offset++) {
                if (arg2[arg2Offset] != 0) {
                    return -1;
                }
            }
            length = arg1.length;
        } else {
            length = arg1.length;
        }

        for (int i = 0; i < length; i++) {
            int b1 = arg1[arg1Offset + i] & 0xFF;
            int b2 = arg2[arg2Offset + i] & 0xFF;
            if (b1 > b2) {
                return 1;
            } else if (b1 < b2) {
                return -1;
            }
        }
        return 0;
    }

    // Used only by computeBigDecimalPrecision()
    // byte array of 1, 10, 100, 1000, 10000, ..., 10^31 for
    // fast computing the length a BigDecimal.
    private static byte[][] tenRadixArr = {
    {(byte) 0x01}, // 10^0
    {(byte) 0x0A}, // 10^1
    {(byte) 0x64}, // 10^2
    {(byte) 0x03, (byte) 0xe8}, // 10^3
    {(byte) 0x27, (byte) 0x10}, // 10^4
    {(byte) 0x01, (byte) 0x86, (byte) 0xa0}, // 10^5
    {(byte) 0x0f, (byte) 0x42, (byte) 0x40}, // 10^6
    {(byte) 0x98, (byte) 0x96, (byte) 0x80}, // 10^7
    {(byte) 0x05, (byte) 0xf5, (byte) 0xe1, (byte) 0x00}, // 10^8
    {(byte) 0x3b, (byte) 0x9a, (byte) 0xca, (byte) 0x00}, // 10^9
    {(byte) 0x02, (byte) 0x54, (byte) 0x0b, (byte) 0xe4, (byte) 0x00}, // 10^10
    {(byte) 0x17, (byte) 0x48, (byte) 0x76, (byte) 0xe8, (byte) 0x00}, // 10^11
    {(byte) 0xe8, (byte) 0xd4, (byte) 0xa5, (byte) 0x10, (byte) 0x00}, // 10^12
    {(byte) 0x09, (byte) 0x18, (byte) 0x4e, (byte) 0x72, (byte) 0xa0, (byte) 0x00}, // 10^13
    {(byte) 0x5a, (byte) 0xf3, (byte) 0x10, (byte) 0x7a, (byte) 0x40, (byte) 0x00}, // 10^14
    {(byte) 0x03, (byte) 0x8d, (byte) 0x7e, (byte) 0xa4, (byte) 0xc6, (byte) 0x80, (byte) 0x00}, // 10^15
    {(byte) 0x23, (byte) 0x86, (byte) 0xf2, (byte) 0x6f, (byte) 0xc1, (byte) 0x00, (byte) 0x00}, // 10^16
    {(byte) 0x01, (byte) 0x63, (byte) 0x45, (byte) 0x78, (byte) 0x5d, (byte) 0x8a, (byte) 0x00, (byte) 0x00}, // 10^17
    {(byte) 0x0d, (byte) 0xe0, (byte) 0xb6, (byte) 0xb3, (byte) 0xa7, (byte) 0x64, (byte) 0x00, (byte) 0x00}, // 10^18
    {(byte) 0x8a, (byte) 0xc7, (byte) 0x23, (byte) 0x04, (byte) 0x89, (byte) 0xe8, (byte) 0x00, (byte) 0x00}, // 10^19
    {(byte) 0x05, (byte) 0x6b, (byte) 0xc7, (byte) 0x5e, (byte) 0x2d, (byte) 0x63, (byte) 0x10, (byte) 0x00, (byte) 0x00}, // 10^20
    {(byte) 0x36, (byte) 0x35, (byte) 0xc9, (byte) 0xad, (byte) 0xc5, (byte) 0xde, (byte) 0xa0, (byte) 0x00, (byte) 0x00}, // 10^21
    {(byte) 0x02, (byte) 0x1e, (byte) 0x19, (byte) 0xe0, (byte) 0xc9, (byte) 0xba, (byte) 0xb2, (byte) 0x40, (byte) 0x00, (byte) 0x00}, // 10^22
    {(byte) 0x15, (byte) 0x2d, (byte) 0x02, (byte) 0xc7, (byte) 0xe1, (byte) 0x4a, (byte) 0xf6, (byte) 0x80, (byte) 0x00, (byte) 0x00}, // 10^23
    {(byte) 0xd3, (byte) 0xc2, (byte) 0x1b, (byte) 0xce, (byte) 0xcc, (byte) 0xed, (byte) 0xa1, (byte) 0x00, (byte) 0x00, (byte) 0x00}, // 10^24
    {(byte) 0x08, (byte) 0x45, (byte) 0x95, (byte) 0x16, (byte) 0x14, (byte) 0x01, (byte) 0x48, (byte) 0x4a, (byte) 0x00, (byte) 0x00, (byte) 0x00}, // 10^25
    {(byte) 0x52, (byte) 0xb7, (byte) 0xd2, (byte) 0xdc, (byte) 0xc8, (byte) 0x0c, (byte) 0xd2, (byte) 0xe4, (byte) 0x00, (byte) 0x00, (byte) 0x00}, // 10^26
    {(byte) 0x03, (byte) 0x3b, (byte) 0x2e, (byte) 0x3c, (byte) 0x9f, (byte) 0xd0, (byte) 0x80, (byte) 0x3c, (byte) 0xe8, (byte) 0x00, (byte) 0x00, (byte) 0x00}, // 10^27
    {(byte) 0x20, (byte) 0x4f, (byte) 0xce, (byte) 0x5e, (byte) 0x3e, (byte) 0x25, (byte) 0x02, (byte) 0x61, (byte) 0x10, (byte) 0x00, (byte) 0x00, (byte) 0x00}, // 10^28
    {(byte) 0x01, (byte) 0x43, (byte) 0x1e, (byte) 0x0f, (byte) 0xae, (byte) 0x6d, (byte) 0x72, (byte) 0x17, (byte) 0xca, (byte) 0xa0, (byte) 0x00, (byte) 0x00, (byte) 0x00}, // 10^29
    {(byte) 0x0c, (byte) 0x9f, (byte) 0x2c, (byte) 0x9c, (byte) 0xd0, (byte) 0x46, (byte) 0x74, (byte) 0xed, (byte) 0xea, (byte) 0x40, (byte) 0x00, (byte) 0x00, (byte) 0x00}, // 10^30
    {(byte) 0x7e, (byte) 0x37, (byte) 0xbe, (byte) 0x20, (byte) 0x22, (byte) 0xc0, (byte) 0x91, (byte) 0x4b, (byte) 0x26, (byte) 0x80, (byte) 0x00, (byte) 0x00, (byte) 0x00},  // 10^31
    {(byte) 0x04, (byte) 0xee, (byte) 0x2d, (byte) 0x6d, (byte) 0x41, (byte) 0x5b, (byte) 0x85, (byte) 0xac, (byte) 0xef, (byte) 0x81, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00}, // 10^32
    {(byte) 0x31, (byte) 0x4d, (byte) 0xc6, (byte) 0x44, (byte) 0x8d, (byte) 0x93, (byte) 0x38, (byte) 0xc1, (byte) 0x5b, (byte) 0x0a, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00}, // 10^33
    {(byte) 0x01, (byte) 0xed, (byte) 0x09, (byte) 0xbe, (byte) 0xad, (byte) 0x87, (byte) 0xc0, (byte) 0x37, (byte) 0x8d, (byte) 0x8e, (byte) 0x64, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00}, // 10^34
    {(byte) 0x13, (byte) 0x42, (byte) 0x61, (byte) 0x72, (byte) 0xc7, (byte) 0x4d, (byte) 0x82, (byte) 0x2b, (byte) 0x87, (byte) 0x8f, (byte) 0xe8, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00}, // 10^35
    {(byte) 0x00, (byte) 0xc0, (byte) 0x97, (byte) 0xce, (byte) 0x7b, (byte) 0xc9, (byte) 0x07, (byte) 0x15, (byte) 0xb3, (byte) 0x4b, (byte) 0x9f, (byte) 0x10, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00}, // 10^36
    {(byte) 0x07, (byte) 0x85, (byte) 0xee, (byte) 0x10, (byte) 0xd5, (byte) 0xda, (byte) 0x46, (byte) 0xd9, (byte) 0x00, (byte) 0xf4, (byte) 0x36, (byte) 0xa0, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00}, // 10^37
    {(byte) 0x4b, (byte) 0x3b, (byte) 0x4c, (byte) 0xa8, (byte) 0x5a, (byte) 0x86, (byte) 0xc4, (byte) 0x7a, (byte) 0x09, (byte) 0x8a, (byte) 0x22, (byte) 0x40, (byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x00} // 10^38
    };


    // If the input string is short, pad it with blanks.
    // If the input string is long, truncate it.
    static public String padOrTruncate(String s, int fixedLength) {
        if (s.length() >= fixedLength) // we need to truncate
        {
            return s.substring(0, fixedLength);
        } else { // we need to pad
            StringBuilder buffer = new StringBuilder(s);
            for (int i = 0; i < fixedLength - s.length(); i++) {
                buffer.append(" ");
            }
            return buffer.toString();
        }
    }

    static public void checkForNegativePositiveSqlcard(Sqlca sqlca, Statement statement) throws SqlException {
        if (sqlca != null) {
            int sqlcode = sqlca.getSqlCode();
            if (sqlcode < 0) {
                throw new SqlException(statement.agent_.logWriter_, sqlca);
            } else {
                if (sqlcode > 0) {
                    statement.accumulateWarning(new SqlWarning(statement.agent_.logWriter_, sqlca));
                }
            }
        }
    }

    static public void checkForNegativePositiveSqlcard(Sqlca sqlca, ResultSet resultSet) throws SqlException {
        if (sqlca != null) {
            int sqlcode = sqlca.getSqlCode();
            if (sqlcode < 0) {
                throw new SqlException(resultSet.agent_.logWriter_, sqlca);
            } else {
                if (sqlcode > 0) {
                    resultSet.accumulateWarning(new SqlWarning(resultSet.agent_.logWriter_, sqlca));
                }
            }
        }
    }

    static public int getSqlcodeFromSqlca(Sqlca sqlca) {
        if (sqlca == null) {
            return 0;
        }
        return sqlca.getSqlCode();
    }

    static public int getUpdateCountFromSqlcard(Sqlca sqlca) {
        if (sqlca == null) {
            return 0;
        } else {
            return sqlca.getUpdateCount();
        }
    }

    // latestException is assumed to be non-null, accumulatedExceptions can be null
    public static SQLException accumulateSQLException(SQLException latestException,
                                                      SQLException accumulatedExceptions) {
        if (accumulatedExceptions == null) {
            return latestException;
        } else {
            accumulatedExceptions.setNextException(latestException);
            return accumulatedExceptions;
        }
    }

    public static SqlException accumulateSQLException(SqlException latestException,
                                                      SqlException accumulatedExceptions) {
        if (accumulatedExceptions == null) {
            return latestException;
        } else {
            accumulatedExceptions.setNextException(latestException);
            return accumulatedExceptions;
        }
    }

    // latestException is assumed to be non-null, accumulatedExceptions can be null
    public static SqlWarning accumulateSQLWarning(SqlWarning latestException,
                                                  SqlWarning accumulatedExceptions) {
        latestException.setNextException(accumulatedExceptions);
        return latestException;
    }

    // just a thought...
    static String getSQLTypeName(int sqlType) {
        switch (sqlType) {
        case java.sql.Types.BIGINT:
            return "BIGINT";
        case java.sql.Types.BINARY:
            return "BINARY";
        case java.sql.Types.BIT:
            return "BIT";
        case java.sql.Types.CHAR:
            return "CHAR";
        case java.sql.Types.DATE:
            return "DATE";
        case java.sql.Types.DECIMAL:
            return "DECIMAL";
        case java.sql.Types.DOUBLE:
            return "DOUBLE";
        case java.sql.Types.REAL:
            return "REAL";
        case java.sql.Types.INTEGER:
            return "INTEGER";
        case java.sql.Types.LONGVARBINARY:
            return "LONGVARBINARY";
        case java.sql.Types.LONGVARCHAR:
            return "LONGVARCHAR";
        case java.sql.Types.NULL:
            return "NULL";
        case java.sql.Types.NUMERIC:
            return "NUMERIC";
        case java.sql.Types.OTHER:
            return "OTHER";
        case java.sql.Types.FLOAT:
            return "FLOAT";
        case java.sql.Types.SMALLINT:
            return "SMALLINT";
        case java.sql.Types.TIME:
            return "TIME";
        case java.sql.Types.TIMESTAMP:
            return "TIMESTAMP";
        case java.sql.Types.TINYINT:
            return "TINYINT";
        case java.sql.Types.VARBINARY:
            return "VARBINARY";
        case java.sql.Types.VARCHAR:
            return "VARCHAR";
        default:
            return null;
        }
    }

    public static boolean isSqlTypeNullable(int sqlType) {
        return (sqlType | 0x01) == sqlType;
    }

    public static int getNonNullableSqlType(int sqlType) {
        return sqlType & ~1;
    }
}
