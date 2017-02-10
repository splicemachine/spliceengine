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
