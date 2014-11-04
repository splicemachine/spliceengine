package com.splicemachine.derby.test.framework;

import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public abstract class SpliceDataWatcher extends TestWatcher {

	@Override
	abstract protected void starting(Description description); 
}
