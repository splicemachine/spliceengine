/*
 * ddlUtils is a subproject of the Apache DB project, and is licensed under
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
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.ddlutils.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Helper class containing string utility functions.
 *
 * @version $Revision: $
 */
public class StringUtilsExt extends org.apache.commons.lang.StringUtils {
    /**
     * Compares the two given strings in a case sensitive or insensitive manner
     * depending on the <code>caseSensitive</code> parameter.
     *
     * @param strA          The first string
     * @param strB          The second string
     * @param caseSensitive Whether case matters in the comparison
     * @return <code>true</code> if the two strings are equal
     */
    public static boolean equals(String strA, String strB, boolean caseSensitive) {
        return caseSensitive ? equals(strA, strB) : equalsIgnoreCase(strA, strB);
    }

    /**
     * Remove the first occurrence of <code>string</code> from the array <code>from</code>, if present.
     * @param string the string to remove
     * @param from the array from which to remove
     * @return a new copy of the array with the first occurrence of <code>string</code> removed,
     * or identical if the string was not contained.
     */
    public static String[] remove(String string, String[] from) {
        List<String> result = new ArrayList<>(Arrays.asList(from));
        result.remove(string);
        return result.toArray(new String[result.size()]);
    }

    /**
     * Does <code>in</code> contain the string <code>string</code>?
     * @param string string to search for
     * @param in string array in which to search
     * @return <code>true</code> iff there was a case-sensitive match
     */
    public static boolean contains(String string, String[] in) {
        for (String str : in) {
            if (str.equals(string)) return true;
        }
        return false;
    }
}
