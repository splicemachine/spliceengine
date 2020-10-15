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

package com.splicemachine.db.impl.ast;

import splice.com.google.common.base.Joiner;
import splice.com.google.common.collect.Lists;

import java.util.List;

/**
 * Utility classes related to Marshalling entities
 *
 * @author Scott Fines
 * Created: 2/1/13 9:36 AM
 */
public class StringUtils {

	/**
	 * Removes from {@code string} all instances of the substring {@code stringToRemove} that
	 * are not prefixed by {@code escapeChar}.
	 *
	 * @param string the string to operate on
	 * @param stringToRemove the string to remove
	 * @param escapeChar the escape character
	 * @return a new String which has been stripped of all {@stringToRemove} substrings which
	 * were not prefixed by {@escapeChar}
	 */
	public static String strip(String string, String stringToRemove,char escapeChar){
		char[] chars = string.toCharArray();
		char[] stripChars = stringToRemove.toCharArray();
		if(stripChars.length>chars.length)return string;
		StringBuilder sb = new StringBuilder();

		int stringLen = stripChars.length;
		int startPos =stringLen;
		boolean escaped = false;
		if(chars[0] == escapeChar){
			escaped = true;
			startPos +=1; //skip the first char if it's the escape character, otherwise we need to check it
		}
		for(int i = startPos;i<chars.length;i++){
			if(i-stringLen>0){
				escaped = chars[i-stringLen-1] == escapeChar;
			}
			if(escaped || !offsetEquals(chars,i-stringLen,stripChars,0,stringLen))
				sb.append(chars,i-stringLen,stringLen);
		}
		return sb.toString();
	}

    /**
     * Parses control characters into actual ISOControl Characters.
     *
     * For example, if {@code controlChars = "\t"}, then the output will be
     * just \t. (E.g. the String representation "\t" will be transformed into \u0009
     * --the unicode tab delimiter).
     *
     * @param controlChars the controlChar representation
     * @return the same string, with any ISOControl character representations replaced
     * with actual unicode values.
     */
    public static String parseControlCharacters(String controlChars){
        char[] chars = controlChars.toCharArray();
        StringBuilder sb = new StringBuilder();
        char[] unicode = null;
        int pos=0;
        iterator:
        while(pos < chars.length){
            if(chars[pos]!='\\'){
                sb.append(chars[pos]);
                pos++;
                continue;
            }
            pos++;
            if(pos==chars.length){
                sb.append(toControlCharacter(chars[pos]));
                pos++;
            }else if(chars[pos]=='u'){
                if(unicode==null)unicode = new char[4];
                for(int i=0;i<4;i++){
                    pos++;
                    if(pos==chars.length){
                        //we don't have enough characters for unicode, so just append everything back
                        //and bail
                        sb.append("\\u");
                        for(int j=0;j<i;j++){
                            sb.append(unicode[j]);
                        }
                        continue iterator;
                    }
                    unicode[i] = chars[pos];
                }
                sb.append(unicodeToChar(unicode));
                pos++;
            }else{
                sb.append(toControlCharacter(chars[pos]));
                pos++;
            }
        }
        return sb.toString();
    }

    private static String unicodeToChar(char[] unicode){
        return String.valueOf((char) Integer.parseInt(String.valueOf(unicode), 16));
    }

    private static char toControlCharacter(char letter){
        switch(letter){
            case 't': return '\t';
            case 'n': return '\n';
            case 'r': return '\r';
            case '0': return '\0';
            default:
                return letter; //can't parse it
        }
    }


	private static boolean offsetEquals(char[] one, int oneOffset,char[] two,int twoOffset, int len){
		if(one.length-oneOffset < len || two.length - twoOffset < len) return false;

		for(int i = oneOffset,j = twoOffset;i<oneOffset+len&&j<twoOffset+ len;i++,j++){
			if(one[i] != two[j]) return false;
		}
		return true;
	}


    /*
     * Create string list with items separated by commas
     * Lists of size 2 joined with conjunction (no comma), lists of size > 2
     * have last item preceded by conjunction
     */
    public static String asEnglishList(List<String> items, String conjunction) {
        int size = items.size();
        if (size == 0) {
            return "";
        } else if (size == 1) {
            return items.get(0);
        } else if (size == 2) {
            return String.format("%s %s %s", items.get(0), conjunction, items.get(1));
        }
        List<String> copy = Lists.newArrayList(items);
        copy.set(size - 1, String.format("%s %s", conjunction, copy.get(size - 1)));
        return Joiner.on(", ").join(copy);
    }
    
    /*
     * Formats time
     */
    public static String formatTime(long sec){
       	long days = sec / (3600 * 24);
    	sec -= days * 3600 * 24;
    	long hours = sec / 3600 ;
    	sec -= hours * 3600;
    	long mins = sec / 60;
    	sec -= mins * 60;
    	long secs = sec;
    	return String.format("%sd %sh %sm %ss", days, hours, mins, secs);
    }

    public static String replace(String text, String searchString, String replacement) {
        return replace(text, searchString, replacement, -1);
    }

    public static String replace(String text, String searchString, String replacement, int max) {
        if (text.isEmpty() || searchString.isEmpty() || replacement == null || max == 0) {
            return text;
        }
        int start = 0;
        int end = text.indexOf(searchString, start);
        if (end == -1) {
            return text;
        }
        int replLength = searchString.length();
        int increase = replacement.length() - replLength;
        increase = (increase < 0 ? 0 : increase);
        increase *= (max < 0 ? 16 : (max > 64 ? 64 : max));
        StringBuilder buf = new StringBuilder(text.length() + increase);
        while (end != -1) {
            buf.append(text.substring(start, end)).append(replacement);
            start = end + replLength;
            if (--max == 0) {
                break;
            }
            end = text.indexOf(searchString, start);
        }
        buf.append(text.substring(start));
        return buf.toString();
    }
}
