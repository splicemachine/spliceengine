package com.ir.hbase.hive.index;

public class ScannerKeyElement {
	private byte[] value;
	private Boolean increment;
	public ScannerKeyElement() {
		super();
	}
	public ScannerKeyElement(byte[] value) {
		super();
		this.value = value;
		this.increment = false;
	}
	public ScannerKeyElement(byte[] value, Boolean increment) {
		super();
		this.value = value;
		this.increment = increment;
	}

	/**
	 * 
	 * return the object pertaining this element 
	 * 
	 * @return the object
	 */
	public byte[] getBytes() {
		return value;
	}
	/**
	 * 
	 * set the object pertaining this element
	 * 
	 * @param object
	 */
	public void setBytes(byte[] value) {
		this.value = value;
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
	
}
