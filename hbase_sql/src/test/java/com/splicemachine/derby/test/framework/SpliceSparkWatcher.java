/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.test.framework;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import com.splicemachine.utils.SpliceLogUtils;

public class SpliceSparkWatcher extends TestWatcher {
	private static final Logger LOG = Logger.getLogger(SpliceSparkWatcher.class);
	public JavaSparkContext jsc;
	public String appName;
	
	public SpliceSparkWatcher(String appName) {
		this.appName = appName;
	}
	
	@Override
	protected void starting(Description description) {
		super.starting(description);
		SpliceLogUtils.trace(LOG, "starting spark");
    	SparkConf sparkConf = new SparkConf().setAppName(appName).setMaster("local");
    	sparkConf.set("spark.broadcast.compress", "false"); // Will attempt to use Snappy without this set.
    	sparkConf.set("spark.driver.allowMultipleContexts", "true"); // SPARK-2243
    	jsc = new JavaSparkContext(sparkConf);
	}

	@Override
	protected void finished(Description description) {
		super.finished(description);
		SpliceLogUtils.trace(LOG, "stopping spark");
		jsc.stop();
	}
}
