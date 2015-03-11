package com.splicemachine.mrio.api;

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
		return String.format("nameType={name=%s, type=%s",name,type);
	}
	
}
