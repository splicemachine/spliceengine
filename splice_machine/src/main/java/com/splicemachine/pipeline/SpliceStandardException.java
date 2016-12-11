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

package com.splicemachine.pipeline;

import com.splicemachine.db.iapi.error.StandardException;
/**
 * 
 * Serializes the key elements of a Derby Standard Exception
 * 
 * @see com.splicemachine.db.iapi.error.StandardException
 *
 */
public class SpliceStandardException extends Exception{
	private static final long serialVersionUID = -298352016321581086L;
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
