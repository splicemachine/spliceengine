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

//ZipInfoProperties

package com.splicemachine.db.impl.tools.sysinfo;

import com.splicemachine.db.iapi.services.info.ProductVersionHolder;

public class ZipInfoProperties // extends Properties
{
	private final ProductVersionHolder	version;
    /**
        full path to zip (or expanded zip)
        C:/db/lib/tools.zip
            -or-
        D:\myWorkDir\db\lib\ *expanded*

        The base name (at the end) should be the same as the zipNameString
     */
    private  String location;

	ZipInfoProperties(ProductVersionHolder version) {
		this.version = version;
	}

	/**
		Method to get only the "interesting" pieces of information
        for the customer, namely the version number (2.0.1) and
		the beta status and the build number
		@return a value for displaying to the user via Sysinfo
    */
    public String getVersionBuildInfo()
    {
        if (version == null)
		{
			return Main.getTextMessage ("SIF04.C");
		}

		if ("DRDA:jcc".equals(version.getProductTechnologyName()))
			return version.getSimpleVersionString() + " - (" + version.getBuildNumber() + ")";

		return version.getVersionBuildString(true);

    }

    public String getLocation()
    {
		if (location == null)
			return Main.getTextMessage ("SIF01.H");
        return location;
    }

	void setLocation(String location) {
		this.location = location;
	}



}


