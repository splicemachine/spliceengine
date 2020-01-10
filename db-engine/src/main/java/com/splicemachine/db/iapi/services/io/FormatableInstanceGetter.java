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

package com.splicemachine.db.iapi.services.io;

import com.splicemachine.db.iapi.services.loader.InstanceGetter;

/**
 * Class that loads Formattables (typically from disk)through
 * one level of indirection.
 * A concrete implementation of this class is registered as the
 * class to handle a number of format identifiers in RegisteredFormatIds.
 * When the in-memory representation of RegisteredFormatIds is set up
 * an instance of the concrete class will be created for each format
 * identifier the class is registered for, and each instances will
 * have its setFormatId() called once with the appropriate format identifier.
 * 
 * <BR>
 * When a Formattable object is read from disk and its registered class
 * is an instance of FormatableInstanceGetter the getNewInstance() method
 * will be called to create the object.
 * The implementation can use the fmtId field to determine the
 * class of the instance to be returned.
 * <BR>
 * Instances of FormatableInstanceGetter are system wide, that is there is
 * a single set of RegisteredFormatIds per system.
 * 
 * @see RegisteredFormatIds
 */
public abstract class FormatableInstanceGetter implements InstanceGetter {

    /**
     * Format identifier of the object 
     */
	protected int fmtId;

    /**
     * Set the format identifier that this instance will be loading from disk.
    */
	public final void setFormatId(int fmtId) {
		this.fmtId = fmtId;
	}
}
