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
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.impl.store.access;

import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.io.Formatable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class PC_XenaVersion implements Formatable
{
	private static final int XENA_MAJOR_VERSION = 1;
	private static final int XENA_MINOR_VERSION_0 = 0;

	//
	//Persistent state. The default value defined here is 
	//over-ridden by readExternal when reading serialized
	//versions.
	private int minorVersion = XENA_MINOR_VERSION_0;
	

	private boolean isUpgradeNeeded(PC_XenaVersion fromVersion)
	{
		return
			fromVersion == null ||
			getMajorVersionNumber() != fromVersion.getMajorVersionNumber();
	}

	public int getMajorVersionNumber() {return XENA_MAJOR_VERSION;}
	public int getMinorVersionNumber() {return minorVersion;}
	
	public void writeExternal(ObjectOutput out) throws IOException
	{
		out.writeInt(getMajorVersionNumber());
		out.writeInt(getMinorVersionNumber());
	}

	public void readExternal(ObjectInput in) throws IOException
	{
		int majorVersion = in.readInt();
		minorVersion = in.readInt();
	}

	public int getTypeFormatId() {return StoredFormatIds.PC_XENA_VERSION_ID;}

	public String toString() {return getMajorVersionNumber()+"."+getMinorVersionNumber();}
}
