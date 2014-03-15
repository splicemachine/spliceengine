package com.splicemachine.client;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.apache.hadoop.hbase.thrift.generated.Hbase;
import org.junit.Assert;
import org.junit.ClassRule;

import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.File;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

/**
 * @author Scott Fines
 * Date: 3/14/14
 */
public class RocketfuelIT {

		public static void main(String...args) throws Exception{
				File correctKeys = new File(SpliceUnitTest.getResourceDirectory()+"/rf/ad_ids");
				List<String> strings = Files.readLines(correctKeys, Charset.defaultCharset());
				List<Long> correctAdIds = Lists.transform(strings, new Function<String, Long>() {
						@Override
						public Long apply(@Nullable String input) {
								return Long.parseLong(input);
						}
				});


				SpliceWatcher watcher = new SpliceWatcher();
				try{
						TestUtils.executeSqlFile(watcher,SpliceUnitTest.getResourceDirectory()+"/rf/rfSchema2.sql","APP");

						PreparedStatement preparedStatement = watcher.prepareStatement(
										"call SYSCS_UTIL.IMPORT_DATA(?,'apollo_mv_minute',null,?,'|',null,null,null,null,0,null)");
						preparedStatement.setString(1,"APP");
						preparedStatement.setString(2,SpliceUnitTest.getResourceDirectory()+"/rf/a10000000-5k.txt");

						ResultSet resultSet = preparedStatement.executeQuery();
						try{
								Assert.assertTrue(resultSet.next());
								Assert.assertEquals("Incorrect number of files returned!",1,resultSet.getInt(1));
								long numRowsReported =resultSet.getLong(3);
								if(numRowsReported!=correctAdIds.size()){
										System.out.printf("Incorrect number of rows reported! Correct:<%d>, Actual:<%d>%n",correctAdIds.size(),numRowsReported);
								}
								System.out.printf("Number of bad rows reported:%d%n", resultSet.getLong(4));
						}finally{
								resultSet.close();
						}

						resultSet = watcher.getStatement().executeQuery("select count(*) from apollo_mv_minute");
						Assert.assertTrue(resultSet.next());
						Assert.assertEquals("Incorrect number of rows returned!",correctAdIds.size(),resultSet.getLong(1));

						PreparedStatement ps = watcher.prepareStatement("select count(*) from apollo_mv_minute where ad_id = ?");
						int numChecked=0;
						for(Long adId:correctAdIds){
								ps.setLong(1,adId);
								ResultSet rs = ps.executeQuery();
								try{
										Assert.assertTrue(rs.next());
										if(rs.getLong(1)!=1){
												System.out.printf("AdId %d has count %d%n",adId,rs.getLong(1));
										}
								}finally{
										rs.close();
								}
								numChecked++;
								if(numChecked%100==0)
										System.out.printf("Checked %d rows%n",numChecked);
						}
				}finally{
						watcher.closeAll();
						watcher.closeConnections();
				}
		}
}
