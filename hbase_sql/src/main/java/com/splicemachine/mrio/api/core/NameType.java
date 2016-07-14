/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.mrio.api.core;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.TypeId;

/**
 * 
 * Class to wrap the name and type of a column for hive SerDe
 *
 */
public class NameType {
	protected String name;
	protected int type;
	
	public NameType() {
		
	}
	
	public NameType(String name, int type) {
		this.name = name;
		this.type = type;
	}
	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getType() {
		return type;
	}
	public void setType(int type) {
		this.type = type;
	}
	public int getTypeFormatId() throws StandardException{
    	return TypeId.getBuiltInTypeId(type).getNull().getTypeFormatId();
    }

	@Override
	public String toString() {
		return String.format("nameType={name=%s, type=%s}",name,type);
	}
	
}
