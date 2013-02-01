package com.splicemachine.derby.utils;

import java.util.Arrays;

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

	private static boolean offsetEquals(char[] one, int oneOffset,char[] two,int twoOffset, int len){
		if(one.length-oneOffset < len || two.length - twoOffset < len) return false;

		for(int i = oneOffset,j = twoOffset;i<oneOffset+len&&j<twoOffset+ len;i++,j++){
			if(one[i] != two[j]) return false;
		}
		return true;
	}

}
