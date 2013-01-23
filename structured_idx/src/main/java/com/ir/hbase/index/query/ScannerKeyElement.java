package com.ir.hbase.index.query;

/**
 * 
 * This class is a representation of the object back from Scanner
 * 
 * @author jLeach
 *
 */
public class ScannerKeyElement {
	private Object object;
	private Boolean increment;
	private Boolean includeEof;
	public ScannerKeyElement() {
		super();
	}
	public ScannerKeyElement(Object object) {
		super();
		this.object = object;
		this.increment = false;
		this.includeEof = true;
	}
	public ScannerKeyElement(Object object, Boolean increment, Boolean includeEof) {
		super();
		this.object = object;
		this.increment = increment;
		this.includeEof = includeEof;
	}

	/**
	 * 
	 * return the object pertaining this element 
	 * 
	 * @return the object
	 */
	public Object getObject() {
		return object;
	}
	/**
	 * 
	 * set the object pertaining this element
	 * 
	 * @param object
	 */
	public void setObject(Object object) {
		this.object = object;
	}
	/**
	 * 
	 * tell whether or not this Element is incrementable
	 * 
	 * @return increment
	 */
	public Boolean getIncrement() {
		return increment;
	}
	/**
	 * 
	 * set increment variable
	 * 
	 * @param increment
	 */
	public void setIncrement(Boolean increment) {
		this.increment = increment;
	}
	
	public Boolean getIncludeEof() {
		return includeEof;
	}
	
	public void setIncludeEof(Boolean includeEof) {
		this.includeEof = includeEof;
	}
	
}
