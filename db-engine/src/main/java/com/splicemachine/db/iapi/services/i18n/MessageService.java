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

package com.splicemachine.db.iapi.services.i18n;

import com.splicemachine.db.iapi.error.ShutdownException;

import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.text.MessageFormat;

/**
 *  Message Service implementation provides a mechanism for locating
 * messages and substituting arguments for message parameters.
 * It also provides a service for locating property values.
 * <p>
 * It uses the resource bundle mechanism for locating messages based on
 * keys; the preferred form of resource bundle is a property file mapping
 * keys to messages.
 *
 */
public final class MessageService {

	private static final Locale EN = new Locale("en", "US");

	private static BundleFinder finder;

	private MessageService() {}


	public static ResourceBundle getBundleForLocale(Locale locale, String msgId) {
		try {
			return MessageService.getBundleWithEnDefault("com.splicemachine.db.loc.m"+hashString50(msgId), locale);
		} catch (MissingResourceException ignored) {
		}
		return null;
	}


	public static void setFinder(BundleFinder theFinder) {
		finder = theFinder;
	}

	public static String getTextMessage(String messageID) {
		return getCompleteMessage(messageID, (Object[]) null);
	}
	public static String getTextMessage(String messageID, Object a1) {

		return getCompleteMessage(messageID, new Object[]{a1});
	}
	public static String getTextMessage(String messageID, Object a1, Object a2) {
		return getCompleteMessage(messageID, new Object[]{a1, a2});
	}
	public static String getTextMessage(String messageID, Object a1, Object a2, Object a3) {
		return getCompleteMessage(messageID, new Object[]{a1, a2, a3});
	}
	public static String getTextMessage(String messageID, Object a1, Object a2, Object a3, Object a4) {
		return getCompleteMessage(messageID, new Object[]{a1, a2, a3, a4});
	}
    public static String getTextMessage(String messageID, Object a1, Object a2, Object a3, Object a4, Object a5) {
        return getCompleteMessage(messageID, new Object[]{a1, a2, a3, a4, a5});
    }

	/**
	  Transform the message from messageID to the actual error, warning, or
	  info message using the correct locale.

	  <P>
	  The arguments to the messages are passed via an object array, the objects
	  in the array WILL be changed by this class. The caller should NOT get the
	  object back from this array.

	 */
	public static String getCompleteMessage(String messageId, Object[] arguments) {

		try {
			return formatMessage(getBundle(messageId), messageId, arguments, true);
		} catch (MissingResourceException | ShutdownException mre) {
			// message does not exist in the requested locale or the default locale.
			// most likely it does exist in our fake base class _en, so try that.
		}
        return formatMessage(getBundleForLocale(EN, messageId), messageId, arguments, false);
	}

	/**
	  Method used by Derby Network Server to get localized message

	  @param sqlcode	sqlcode, not used.
	  @param errmcLen	sqlerrmc length
	  @param sqlerrmc	sql error message tokens, variable part of error message (ie.,
						arguments) plus messageId, separated by separator.
	  @param sqlerrp	not used
	  @param errd0		not used
	  @param warn		not used
	  @param sqlState	5-char sql state
	  @param file		not used
	  @param localeStr	client locale in string
	  @param msg		OUTPUT parameter, localized error message
	  @param rc			OUTPUT parameter, return code -- 0 for success
	 */
	public static void getLocalizedMessage(int sqlcode, short errmcLen, String sqlerrmc,
										String sqlerrp, int errd0, int errd1, int errd2,
										int errd3, int errd4, int errd5, String warn,
										String sqlState, String file, String localeStr,
										String[] msg, int[] rc)
	{
		//figure out client locale from input locale string

		int _pos1 = localeStr.indexOf("_");		// "_" position

		Locale locale = EN;		//default locale
		if (_pos1 != -1)
		{
		    int _pos2 = localeStr.lastIndexOf("_");
			String language = localeStr.substring(0, _pos1);
			if (_pos2 == _pos1)
			{
				String country = localeStr.substring(_pos1 + 1);
				locale = new Locale(language, country);
			}
			else
			{
				String country = localeStr.substring(_pos1 + 1, _pos2);
				String variant = localeStr.substring(_pos2 + 1);
				locale = new Locale(language, country, variant);
			}
		}

		// get messageId and arguments, messageId is necessary for us to look up
		// localized message from property file.  messageId was sent as the last
		// token in the sqlerrmc.

		String messageId = sqlState; 	//use sqlState if we don't have messageId
		Object[] arguments = null;
		if (sqlerrmc != null && !sqlerrmc.isEmpty())
		{
			char [] sqlerrmc_chars = sqlerrmc.toCharArray();
			int numArgs = 0, lastSepIdx = -1; // last separator index
			for (int i = 0; i < sqlerrmc_chars.length; i++)
			{
				if (sqlerrmc_chars[i] == 20)	// separator
				{
					numArgs++;
					lastSepIdx = i;
				}
			}
			if (numArgs == 0)
				messageId = new String(sqlerrmc_chars); //no args, only messageId then
			else
			{
				messageId = new String(sqlerrmc_chars, lastSepIdx+1, sqlerrmc_chars.length-lastSepIdx-1);
				arguments = new Object[numArgs];
				for (int start = 0, arg = 0, i = 0; i < lastSepIdx + 1; i++)
				{
					if (i == lastSepIdx || sqlerrmc_chars[i] == 20)	// delimiter 
					{
						arguments[arg++] = new String(sqlerrmc_chars, start, i - start);
						start = i + 1;
					}
				}
			}
		}

		try {
			msg[0] = formatMessage(getBundleForLocale(locale, messageId), messageId, arguments, true);
			rc[0] = 0;
			return;
		} catch (MissingResourceException | ShutdownException mre) {
			// message does not exist in the requested locale
			// most likely it does exist in our fake base class _en, so try that.
		}
        msg[0] = formatMessage(getBundleForLocale(EN, messageId), messageId, arguments, false);
		rc[0] = 0;
	}
	
	/**
	  Method used by Derby Network Server to get localized message 

	  @param locale		locale
	  @param messageId	message id
	  @param args		message arguments
	 */
	public static String getLocalizedMessage(Locale locale, String messageId, Object [] args)
	{
		String locMsg = null;

		try {
			locMsg = formatMessage(getBundleForLocale(locale, messageId), messageId, args, true);
			return locMsg;
		} catch (MissingResourceException | ShutdownException mre) {
			// message does not exist in the requested locale
			// most likely it does exist in our fake base class _en, so try that.
		}
        locMsg = formatMessage(getBundleForLocale(EN, messageId), messageId, args, false);
		return locMsg;
	}

	/**
	 */
	public static String getProperty(String messageId, String propertyName) {

		ResourceBundle bundle = getBundle(messageId);

		try {
			if (bundle != null)
				return bundle.getString(messageId + "." + propertyName);
		} catch (MissingResourceException ignored) {
		}
		return null;
	}

	//
	// class implementation
	//
	public static String formatMessage(ResourceBundle bundle, String messageId, Object[] arguments, boolean lastChance) {

		if (arguments == null)
			arguments = new Object[0];

		if (bundle != null) {

			try {
				messageId = bundle.getString(messageId);

				try {
					return MessageFormat.format(messageId, arguments);
				}
				catch (IllegalArgumentException | NullPointerException ignored) {
				}

            } catch (MissingResourceException mre) {
				// caller will try and handle the last chance
				if (lastChance)
					throw mre;
			} 
		}

		if (messageId == null)
			messageId = "UNKNOWN";

		
		StringBuilder sb = new StringBuilder(messageId);

		int len = arguments.length;
		if (len > 0)
			sb.append(" : ");

		for (int i=0; i < len; i++) {
		    // prepend a comma to all but the first
			if (i > 0)
				sb.append(", ");

			sb.append('[');
			sb.append(i);
			sb.append("] ");
			if (arguments[i] == null)
				sb.append("null");
			else
				sb.append(arguments[i].toString());
		}

		
		return sb.toString();
	}

	private static ResourceBundle getBundle(String messageId) {

		ResourceBundle bundle = null;

		if (finder != null)
			bundle = finder.getBundle(messageId);

		if (bundle == null) {
			bundle = MessageService.getBundleForLocale(Locale.getDefault(), messageId);
		}

		return bundle;
	}

	/**
		Method to use instead of ResourceBundle.getBundle().
		This method acts like ResourceBundle.getBundle() but if
		the resource is not available in the requested locale,
		default locale or base class the one for en_US is returned.
	*/

	public static ResourceBundle getBundleWithEnDefault(String resource, Locale locale) {

		try {
			return ResourceBundle.getBundle(resource, locale);
		} catch (MissingResourceException mre) {

			// This covers the case where neither the
			// requested locale or the default locale
			// have a resource.

			return ResourceBundle.getBundle(resource, EN);
		}
	}

	/**
		Hash function to split messages into 50 files based
		upon the message identifier or SQLState. We don't use
		String.hashCode() as it varies between releases and
		doesn't provide an even distribution across the 50 files.

	*/
	public static int hashString50(String key) {
		int hash = 0;
		int len = key.length();
		if (len > 5)
			len = 5;

		for (int i = 0; i < len; i++) {
			hash += key.charAt(i);
		}
		hash = hash % 50;
		return hash;
	}
}
