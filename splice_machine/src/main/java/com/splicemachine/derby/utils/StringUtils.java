package com.splicemachine.derby.utils;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.util.Arrays;
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
                continue iterator;
            }
            pos++;
            if(pos==chars.length){
                sb.append(toControlCharacter(chars[pos]));
                pos++;
                continue iterator;
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
        return String.valueOf((char)Integer.parseInt(String.valueOf(unicode),16));
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

}
