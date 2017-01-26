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

package com.splicemachine.db.iapi.services.classfile;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import java.io.IOException;

/** Constant Pool class - pages 92-99 */
public abstract class ConstantPoolEntry /*implements PoolEntry*/
{
	
	protected int tag; // u1 (page 83)
	protected boolean doubleSlot; // Some entries take up two slots! (see footnote page 98) 

	/* Index within Vector */
	protected int index;

	protected ConstantPoolEntry(int tag) {
		this.tag = tag;
	}

	int getIndex() {
		if (SanityManager.DEBUG) {
			if (index <= 0)
			{
				SanityManager.THROWASSERT("index is expected to be > 0, is " + index);
			}
		}
		return index;
	}

	void setIndex(int index) {
		this.index = index;
	}

	boolean doubleSlot() {
		return doubleSlot;
	}

	/**
		Return the key used to key this object in a hashtable
	*/
	Object getKey() {
		return this;
	}

	/**
		Return an estimate of the size of the constant pool entry.
	*/
	abstract int classFileSize();

	void put(ClassFormatOutput out) throws IOException {
		out.putU1(tag);
	}

	/*
	** Public API methods
	*/

	/**
		Return the tag or type of the entry. Will be equal to one of the
		constants above, e.g. CONSTANT_Class.
	*/
	final int getTag() {
		return tag;
	}

	/**	
		Get the first index in a index type pool entry.
		This call is valid when getTag() returns one of
		<UL> 
		<LI> CONSTANT_Class
		<LI> CONSTANT_Fieldref
		<LI> CONSTANT_Methodref
		<LI> CONSTANT_InterfaceMethodref
		<LI> CONSTANT_String
		<LI> CONSTANT_NameAndType
		</UL>
	*/
	int getI1() { return 0; }

	/**	
		Get the second index in a index type pool entry.
		This call is valid when getTag() returns one of
		<UL> 
		<LI> CONSTANT_Fieldref
		<LI> CONSTANT_Methodref
		<LI> CONSTANT_InterfaceMethodref
		<LI> CONSTANT_NameAndType
		</UL>
	*/	
	int getI2() { return 0; };
}

