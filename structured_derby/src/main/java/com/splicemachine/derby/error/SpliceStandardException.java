package com.splicemachine.derby.error;

import org.apache.derby.iapi.error.StandardException;
/**
 * 
 * Serializes the key elements of a Derby Standard Exception
 * 
 * @author John Leach
 * @see org.apache.derby.iapi.error.StandardException
 *
 */
public class SpliceStandardException {
	public SpliceStandardException() {
		
	}
	public SpliceStandardException (StandardException standardException) {
		this.severity = standardException.getSeverity();
		this.textMessage = standardException.getMessage();
		this.sqlState = standardException.getSqlState();
	}
	
	private int severity;
	private String textMessage;
	private String sqlState;
	public int getSeverity() {
		return severity;
	}
	public void setSeverity(int severity) {
		this.severity = severity;
	}
	public String getTextMessage() {
		return textMessage;
	}
	public void setTextMessage(String textMessage) {
		this.textMessage = textMessage;
	}
	public String getSqlState() {
		return sqlState;
	}
	public void setSqlState(String sqlState) {
		this.sqlState = sqlState;
	}
	
	public StandardException generateStandardException() {
		StandardException se = new StandardException();
		se.setSeverity(severity);
		se.setSqlState(sqlState);
		se.setTextMessage(textMessage);
		return se;
	}
}
