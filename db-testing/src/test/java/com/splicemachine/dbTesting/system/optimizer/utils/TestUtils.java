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
package com.splicemachine.dbTesting.system.optimizer.utils;
/**
 * 
 * Class TestUtils: Utility class for measuring query times
 *
 */
public class TestUtils {
	static int MILLISECONDS_IN_SEC=1000;
	static int SECONDS_IN_MIN=60;
	static int MINUTES_IN_HR=60;
	
	public static String getTime(long timeInMs)
	{
		StringBuffer stringBuff = new StringBuffer(32);
		//get Hours
		int hours = (int)timeInMs /( MINUTES_IN_HR * SECONDS_IN_MIN * MILLISECONDS_IN_SEC);
		if (hours > 0) {
			stringBuff.append(hours);
			stringBuff.append(" hr");
		}
		//get Minutes
		int remainHours = (int)timeInMs % (MINUTES_IN_HR * SECONDS_IN_MIN * MILLISECONDS_IN_SEC);
		int minutes = remainHours / (SECONDS_IN_MIN * MILLISECONDS_IN_SEC);
		if (minutes > 0) {
			stringBuff.append(minutes);
			stringBuff.append(" min ");
		}
		//get Seconds
		int remainMinutes = remainHours % (SECONDS_IN_MIN * MILLISECONDS_IN_SEC);
		int seconds = remainMinutes / MILLISECONDS_IN_SEC;
		int milliseconds = remainMinutes % MILLISECONDS_IN_SEC;

		stringBuff.append(seconds);
		if (hours == 0 && minutes < 5)
		{
			stringBuff.append('.');
			if (milliseconds < 10)
				stringBuff.append('0');
			if (milliseconds < 100)
				stringBuff.append('0');
			stringBuff.append(milliseconds);
		}
		stringBuff.append(" secs ");
		return stringBuff.toString();
	
	}

}
