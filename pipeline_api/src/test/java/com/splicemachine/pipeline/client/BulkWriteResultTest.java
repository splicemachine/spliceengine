/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
//package com.splicemachine.pipeline.impl;
//
//import com.carrotsearch.hppc.IntObjectOpenHashMap;
//import com.carrotsearch.hppc.IntOpenHashSet;
//import com.carrotsearch.hppc.cursors.IntObjectCursor;
//import com.google.common.collect.Lists;
//import com.splicemachine.pipeline.api.Code;
//import com.splicemachine.pipeline.utils.PipelineUtils;
//import org.junit.Assert;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.junit.runners.Parameterized;
//
//import java.util.Collection;
//
///**
// * @author Scott Fines
// *         Date: 2/3/14
// */
//@RunWith(Parameterized.class)
//public class BulkWriteResultTest {
//
//		@Parameterized.Parameters
//		public static Collection<Object[]> data() {
//				Collection<Object[]> data = Lists.newArrayList();
//
//				for(Code code:Code.values()){
//					data.add(new Object[]{code,"testErrorMessage"});
//				}
//				return data;
//		}
//
//		private final Code code;
//		private final String errorMessage;
//
//		public BulkWriteResultTest(Code code, String errorMessage) {
//				this.code = code;
//				this.errorMessage = errorMessage;
//		}
//
//		@Test
//		public void testCanSerializeFailedRowsCorrectly() throws Exception {
//				IntOpenHashSet notRunRows = IntOpenHashSet.newInstanceWithCapacity(10,0.75f);
//				for(int i=10;i<15;i++){
//						notRunRows.add(i);
//				}
//				IntObjectOpenHashMap<WriteResult> failedRows = IntObjectOpenHashMap.newInstance();
//				for(int i=0;i<10;i++){
//						failedRows.put(i,new WriteResult(code,errorMessage));
//				}
//				BulkWriteResult result = new BulkWriteResult(new WriteResult(Code.PARTIAL),notRunRows,failedRows);
//
//				byte[] data = PipelineUtils.toCompressedBytes(result);
//				BulkWriteResult decoded = PipelineUtils.fromCompressedBytes(data, BulkWriteResult.class);
//				Assert.assertEquals(notRunRows, decoded.getNotRunRows());
//				IntObjectOpenHashMap<WriteResult> decodedFailedRows = decoded.getFailedRows();
//				Assert.assertEquals("Incorrect decoded size!", failedRows.size(), decodedFailedRows.size());
//				for(IntObjectCursor<WriteResult> cursor:decodedFailedRows){
//						WriteResult correct = failedRows.get(cursor.key);
//						Assert.assertNotNull("Unexpected returned write result!",correct);
//						Assert.assertEquals("Incorrect returned error code!", cursor.value.getCode(), correct.getCode());
//						if(correct.getCode()== Code.FAILED)
//								Assert.assertEquals("Incorrect returned error message!",cursor.value.getErrorMessage(),correct.getErrorMessage());
//				}
//		}
//}
