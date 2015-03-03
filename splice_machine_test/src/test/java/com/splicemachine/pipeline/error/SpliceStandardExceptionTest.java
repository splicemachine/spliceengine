package com.splicemachine.pipeline.error;

import org.junit.Assert;
import com.splicemachine.db.iapi.error.StandardException;
import org.junit.Test;

import com.splicemachine.pipeline.exception.SpliceStandardException;

public class SpliceStandardExceptionTest {
	@Test 
	public void instanceTest() {
		StandardException se = StandardException.unexpectedUserException(new Exception("Unexpected"));
		SpliceStandardException sse = new SpliceStandardException(se);
		Assert.assertEquals(sse.getSeverity(), se.getSeverity());
		Assert.assertEquals(sse.getSqlState(), se.getSqlState());
		Assert.assertEquals(sse.getTextMessage(), se.getTextMessage());
	}

	@Test 
	public void generateStandardExceptionTest() {
		StandardException se = StandardException.unexpectedUserException(new Exception("Unexpected"));
		SpliceStandardException sse = new SpliceStandardException(se);
		StandardException se2 = sse.generateStandardException();
		Assert.assertEquals(se2.getSeverity(), se.getSeverity());
		Assert.assertEquals(se2.getSqlState(), se.getSqlState());
		Assert.assertEquals(se2.getTextMessage(), se.getTextMessage());
	}

	
}
