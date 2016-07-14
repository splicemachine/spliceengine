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

package com.splicemachine.derby.utils;

import java.util.Arrays;

import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

public class DerbyLogUtils {
	
	private DerbyLogUtils(){}
	
	public static void traceDescriptors(Logger logger,String startMessage,DataValueDescriptor[] descriptors){
		if(logger.isTraceEnabled()){
			String[] descStrs = new String[descriptors.length];
			for(int i=0;i<descStrs.length;i++){
				descStrs[i] = descriptors[i].getTypeName();
			}
			logger.trace(startMessage+":"+Arrays.toString(descStrs));
		}
	}
	
	public static void logIndexKeys(Logger logger, int startSearchOperator, DataValueDescriptor[] startKeys, int stopSearchOperator, DataValueDescriptor[] stopKeys) {
		if (logger.isTraceEnabled())
			SpliceLogUtils.trace(logger, "logIndexKeys");
			try {
				SpliceLogUtils.trace(logger, "startSearchOperator %d",startSearchOperator);
				if (startKeys != null) {
					for (int i =0;i<startKeys.length;i++) {
						if (startKeys[i] ==null) 
							SpliceLogUtils.trace(logger,"startKey is null for position %d",i);
						else
							SpliceLogUtils.trace(logger,"startKey - %s : %s",startKeys[i].getTypeName(),startKeys[i].getTraceString());
					}
				}
				SpliceLogUtils.trace(logger, "stopSearchOperator %d",stopSearchOperator);
				if (stopKeys != null) {
					for (int i =0;i<stopKeys.length;i++)
						if (startKeys[i] ==null) 
							SpliceLogUtils.trace(logger,"stopKey is null for position %d",i);
						else
							SpliceLogUtils.trace(logger,"stopKey - %s : %s",stopKeys[i].getTypeName(),stopKeys[i].getTraceString());
				}				
			} catch (Exception e) {
				SpliceLogUtils.logAndThrowRuntime(logger, e);
			}
	}
}
