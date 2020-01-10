/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
