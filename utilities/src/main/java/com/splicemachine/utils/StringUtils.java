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

package com.splicemachine.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Scott Fines
 *         Date: 7/7/15
 */
public class StringUtils{

    //Match the password part in the call statement
    public static final String maskPasswordPatternStr = "CALL\\s+SYSCS_UTIL\\s*.\\s*(?:(?:SYSCS_CREATE_USER\\s*\\('\\s*(?:[^']*)'\\s*,\\s*'([^']*)'\\s*\\))" +
            "|(?:SYSCS_RESET_PASSWORD\\s*\\(\\s*'(?:[^']*)'\\s*,\\s*'([^']*)'\\s*\\))" +
            "|(?:SYSCS_MODIFY_PASSWORD\\s*\\(\\s*'([^']*)'\\s*\\)))";
    public static final Pattern maskPasswordPattern = Pattern.compile(maskPasswordPatternStr,Pattern.CASE_INSENSITIVE);
    public static final String maskString = "********";

    public static String trimTrailingSpaces(String str){
        if (str == null) {
            return null;
        }
        int len = str.length()-1;

        char ch;
        while(len>=0){
            ch = str.charAt(len);
            if(ch > ' ') break;
            len--;
        }
        return str.substring(0,len+1);
    }

    public static String rightPad(String str, int size, char padChar) {
        if(str == null) {
            return null;
        } else {
            int pads = size - str.length();
            return pads <= 0?str:(pads > 8192?rightPad(str, size, String.valueOf(padChar)): str + padding(pads, padChar));
        }
    }

    public static String rightPad(String str, int size, String padStr) {
        if(str == null) {
            return null;
        } else {
            if(isEmpty(padStr)) {
                padStr = " ";
            }

            int padLen = padStr.length();
            int strLen = str.length();
            int pads = size - strLen;
            if(pads <= 0) {
                return str;
            } else if(padLen == 1 && pads <= 8192) {
                return rightPad(str, size, padStr.charAt(0));
            } else if(pads == padLen) {
                return str + padStr;
            } else if(pads < padLen) {
                return str + padStr.substring(0, pads);
            } else {
                char[] padding = new char[pads];
                char[] padChars = padStr.toCharArray();

                for(int i = 0; i < pads; ++i) {
                    padding[i] = padChars[i % padLen];
                }

                return str + new String(padding);
            }
        }
    }

    public static boolean isEmpty(String str) {
        return str == null || str.isEmpty();
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private static String padding(int repeat, char padChar) throws IndexOutOfBoundsException {
        if(repeat < 0) {
            throw new IndexOutOfBoundsException("Cannot pad a negative amount: " + repeat);
        } else {
            char[] buf = new char[repeat];

            for(int i = 0; i < buf.length; ++i) {
                buf[i] = padChar;
            }

            return new String(buf);
        }
    }

    public static String maskMessage(String message, Pattern maskPattern, String maskString) {
        if (message == null)
            return null;
        if (maskString == null)
            maskString = "";

        Matcher matcher = maskPattern.matcher(message);

        List<Pair<Integer, Integer>> groups = new ArrayList<>();
        while (matcher.find()) {
            for (int i = 1; i <= matcher.groupCount(); i++) {
                if (matcher.group(i) != null)
                    groups.add(new Pair<>(matcher.start(i), matcher.end(i)));
            }
        }
        if (groups.size() == 0) {
            return message;
        }
        StringBuilder maskedMessage = new StringBuilder("");
        for (int i = 0; i <= groups.size(); i++) {
            int start;
            int end;
            if (i == 0) {
                start = 0;
            } else {
                start = groups.get(i-1).getSecond();
                maskedMessage.append(maskString);
            }
            if (i == groups.size()) {
                end = message.length() - 1;
            } else {
                end = groups.get(i).getFirst() - 1;
            }
            maskedMessage.append(message, start, end+1);
        }
        return String.valueOf(maskedMessage);
    }


    public static String logSpliceAuditEvent(String userid, String event, boolean status, String addr, String statement, String reason)
    {

        StringBuilder sb = new StringBuilder();
        sb.setLength(0);
        sb.append("userid=").append(userid).append("\t");
        sb.append("event=").append(event).append("\t");
        sb.append("status=").append(status ? "SUCCEED" : "FAILED").append("\t");
        sb.append("ip=").append(addr).append("\t");
        if (statement != null)
            sb.append("statement=").append(maskMessage(statement, maskPasswordPattern,maskString)).append("\t");
        if (!status)
            sb.append("reason=").append(reason);
        return sb.toString();
    }
}
