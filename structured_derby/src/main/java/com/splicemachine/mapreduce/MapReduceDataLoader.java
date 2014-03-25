package com.splicemachine.mapreduce;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.load.ImportContext;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.utils.ErrorState;
import org.apache.commons.cli.*;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.jdbc.ClientDriver;
import org.apache.derby.jdbc.ClientDriver40;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;

public class MapReduceDataLoader extends Configured implements Tool {		
	
		public static final String HBASE_TABLE_NAME = "hbase.table.name";
		public static Gson gson = new Gson();

		public int runHFileIncrementalLoad(ImportContext importContext,String tempOutputPath,int numReduces) throws Exception {
				Configuration conf = getConf();
				conf.set(HBASE_TABLE_NAME, Long.toString(importContext.getTableId()));
				HBaseConfiguration.addHbaseResources(conf);
				Job job = new Job(conf);
				job.getConfiguration().set(HBaseBulkLoadMapper.IMPORT_CONTEXT, gson.toJson(importContext));
				job.setJarByClass(MapReduceDataLoader.class);
				job.setMapperClass(HBaseBulkLoadMapper.class);
				job.setMapOutputKeyClass(ImmutableBytesWritable.class);
				job.setMapOutputValueClass(ImmutableBytesWritable.class);
				job.setInputFormatClass(TextInputFormat.class);

				HTable hTable = new HTable(job.getConfiguration(),Bytes.toBytes(importContext.getTableId()+""));
				HFileOutputFormat.configureIncrementalLoad(job,hTable);
//				System.out.printf("Setting up reducers with %d reduces%n",numReduces);
				int setReduceTasks = job.getNumReduceTasks();
				if(numReduces>setReduceTasks)
						job.setNumReduceTasks(numReduces);
				job.setReducerClass(HBaseBulkLoadReducer.class);
				job.getConfiguration().set("import.txnId", importContext.getTransactionId());

				FileInputFormat.addInputPath(job, importContext.getFilePath());
				Path outputDir = new Path(tempOutputPath);
				FileOutputFormat.setOutputPath(job, outputDir);
				int jobCode = submitAndCompleteJob(job);
				if(jobCode!=0)
						return jobCode; //if it doesn't succeed, stop here

				//load the files up incrementally
				System.out.println("Loading generated store files");
				LoadIncrementalHFiles loader = new LoadIncrementalHFiles(getConf());
				loader.doBulkLoad(outputDir,hTable);
				return 0;
		}

		protected int submitAndCompleteJob(Job job) throws IOException, InterruptedException, ClassNotFoundException {
				long startTime = System.currentTimeMillis();
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//				job.waitForCompletion(true);
				job.submit();
				System.out.printf("Job started at %s%n", formatter.format(new Date(startTime)));
				System.out.println("Job Information: ");
				System.out.println(job);
				float previousStatus = 0.0f;
				while(!job.isComplete()){
/*
						Thread.sleep(500l);
						float status;
						status = job.getStatus().getMapProgress();
						if(status>=1.0f){
								status = job.getStatus().getReduceProgress();
						}
						if(Math.abs(status-previousStatus)>0.01){
								System.out.printf("Map Progress: %f\tReduce Progress: %f%n",job.getStatus().getMapProgress(),job.getStatus().getReduceProgress());
								previousStatus = status;
						}
*/
				}
				long finishTime = System.currentTimeMillis();
				System.out.printf("Job ended at %s%n", formatter.format(new Date(finishTime)));
				System.out.printf("Elapsed job time: %f s%n", (finishTime - startTime) / (1000d));

				return job.isSuccessful()? 0: 5;
//				if(!job.isSuccessful()) return 5;
//				return 0;
//				switch(job.getJobState()){
//						case FAILED:
//								System.err.printf("Job failed %n");
//								return 5;
//						case KILLED:
//								System.err.printf("Job killed %n");
//								return 6;
//						default:
//								return 0;
//				}
		}

		public static void main(String...args) throws Exception{
				MapReduceDataLoader loader = new MapReduceDataLoader();
				int run = ToolRunner.run(loader, args);
				System.exit(run);
		}


		@Override
		public int run(String[] args) throws Exception {
				/*
				 * We have to build the ImportContext so that we can use the Import
				 * code correctly. In order to do this, we execute the following:
				 *
				 * 1. Open a JDBC connection.
				 * 2. Use JDBC queries to get the Import information necessary
				 * 3. Build the ImportContext
				 * 4. Submit and monitor the job for completion. We submit the job,
				 * then every so often, we will print progress information until
				 * it completes or fails
				 */

				Options options = getOptions();
				CommandLine commandLine;
				try{
					commandLine = new BasicParser().parse(options,args);
				}catch(ParseException pe){
						printUsage(options);
						return 65; //error code for BAD_ARGS
				}
				//make sure that all the import context settings parse well before we try connecting to anything
				ImportContext.Builder ctxBuilder = new ImportContext.Builder();
				ctxBuilder = ctxBuilder.colDelimiter(commandLine.getOptionValue("d",","))
								.stripCharacters(commandLine.getOptionValue("q"))
								.timestampFormat(commandLine.getOptionValue("TS"))
								.timeFormat(commandLine.getOptionValue("T"))
								.dateFormat(commandLine.getOptionValue("D"));

				Path path = new Path(commandLine.getOptionValue("i"));
				FileSystem fs = FileSystem.get(getConf());
				if(!fs.exists(path)){
						printSqlException("",
										PublicAPI.wrapStandardException(ErrorState.DATA_FILE_NOT_FOUND.newException(path)));
						return 2;
				}
				ctxBuilder = ctxBuilder.path(path);

				Connection connection = null;
				boolean failed = false;
				try{
						try{
								connection = jdbcConnect(commandLine);
						}catch(ParseException pe){
								failed=true;
								return 65;
						}catch(SQLException se){
								failed = true;
								return 1;
						}

						try{
								ctxBuilder = ctxBuilder.transactionId(Long.toString(getTransactionInformation(connection)));
						}catch(SQLException se){
								failed = true;
								printSqlException("Unable to get transaction information",se);
								return 2;
						}

						try{
								addTableAndSchemaInformation(connection,ctxBuilder,commandLine);
						}catch(SQLException se){
								failed=true;
								printSqlException("Unable to establish table and schema information", se);
								return 3;
						}

						try{
								addColumnInformation(connection,ctxBuilder,commandLine);
						}catch(SQLException se){
								failed = true;
								printSqlException("Unable to establish column information",se);
								return 4;
						}
						ImportContext ctx = ctxBuilder.build();
						int numReducers;
						try{
								numReducers = Integer.parseInt(commandLine.getOptionValue("r","2"));
						}catch(NumberFormatException nfe){
								System.err.printf("Could not parse reducers %s, using default of 2%n",commandLine.getOptionValue("r"));
								numReducers=2;
						}
						return runHFileIncrementalLoad(ctx,commandLine.getOptionValue("o"),numReducers);
				}finally{
						commitAndClose(connection, failed);
				}
		}

		private void commitAndClose(Connection connection, boolean failed) {
				if(connection==null) return;
				try{
						if(failed)
								connection.rollback();
						else
								connection.commit();
				} catch (SQLException e) {
						printSqlException("Error finalizing transaction",e);
				}finally{
						try {
								connection.close();
						} catch (SQLException e) {
								printSqlException("Unable to close connection safely",e);
						}
				}
		}

		private long getTransactionInformation(Connection connection)  throws SQLException{
				PreparedStatement ps = null;
				try{
						ps = connection.prepareStatement("call SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()");
						ResultSet rs = ps.executeQuery();
						rs.next();
						return rs.getLong(1);
				}finally{
						if(ps!=null)
								ps.close();
				}
		}

		private void addColumnInformation(Connection connection, ImportContext.Builder ctxBuilder, CommandLine commandLine) throws SQLException {
				String importColumnList = commandLine.getOptionValue("c");
				String schemaName = commandLine.getOptionValue("s","APP").toUpperCase();
				String tableName = commandLine.getOptionValue("t").toUpperCase();

				//get AutoIncrement locations, if any
				PreparedStatement ps = null;
				try{
						ps = connection.prepareStatement("call SYSCS_UTIL.SYSCS_GET_AUTO_INCREMENT_ROW_LOCATIONS(?,?)");
						ps.setString(1,schemaName);
						ps.setString(2,tableName);
						ResultSet resultSet = ps.executeQuery();

						List<byte[]> autoIncLocs = Lists.newArrayList();
						while(resultSet.next()){
								String hex = resultSet.getString(1);
								if(!resultSet.wasNull())
										autoIncLocs.add(BytesUtil.fromHex(hex));
								else
										autoIncLocs.add(null);
						}
						byte[][] autoIncLocations = new byte[autoIncLocs.size()][];
						autoIncLocs.toArray(autoIncLocations);

						ImportUtils.buildColumnInformation(connection,schemaName,tableName,importColumnList,ctxBuilder,autoIncLocations);
				}finally{
						if(ps!=null)
								ps.close();
				}
		}

		private void printSqlException(String errorMsg,SQLException e) {
				System.err.printf("[%s] %s%n%s%n",e.getSQLState(),errorMsg,e.getMessage());
		}

		private void addTableAndSchemaInformation(Connection connection, ImportContext.Builder ctxBuilder,CommandLine cli) throws SQLException {
				String schemaName = cli.getOptionValue("s","APP").toUpperCase();
				String tableName = cli.getOptionValue("t").toUpperCase();
				//get the conglomerate number for the table. If it doesn't exist, then the table doesn't exist. Report it and fail
				PreparedStatement ps = null;
				try{
				ps = connection.prepareStatement("select c.conglomeratenumber\n" +
								"from\n" +
								"        sys.systables t,\n" +
								"        sys.sysschemas s,\n" +
								"        sys.sysconglomerates c\n" +
								"where\n" +
								"        t.tableid = c.tableid\n" +
								"        and t.schemaid = s.schemaid\n" +
								"        and t.tablename = ?\n" +
								"				 and s.schemaname = ?\n" +
								"        and c.isindex = false");
						ps.setString(1,tableName);
						ps.setString(2,schemaName);
						ResultSet resultSet = ps.executeQuery();
						//validate that the table exists
						if(!resultSet.next()){
								throw PublicAPI.wrapStandardException(ErrorState.LANG_TABLE_NOT_FOUND.newException(schemaName+"."+tableName));
						}
						ctxBuilder.destinationTable(resultSet.getInt(1));
				}finally{
						if(ps!=null)
								ps.close();
				}
		}

		protected Connection jdbcConnect(CommandLine commandLine) throws ParseException, SQLException {
				//connect to JDBC
				String jdbcHost = commandLine.getOptionValue("j");
				int jdbcPort;
				try{
						jdbcPort = Integer.parseInt(commandLine.getOptionValue("p","1527"));
				}catch(NumberFormatException nfe){
						System.err.printf("Invalid jdbc port: %s%n", commandLine.getOptionValue("p"));
						throw new ParseException(nfe.getMessage());
				}

				String connectStr = String.format("jdbc:splice://%s:%d/splicedb;",jdbcHost,jdbcPort);
				try{
						ClientDriver driver = new ClientDriver40();
						Connection connection = driver.connect(connectStr, new Properties());
						connection.setAutoCommit(false);
						return connection;
				}catch(SQLException se){
						printSqlException("Unable to connect to database located at " + jdbcHost + ":" + jdbcPort, se);
						throw se;
				}
		}

		private void printUsage(Options options) {
				HelpFormatter helpFormatter = new HelpFormatter();
				helpFormatter.setWidth(80);
				helpFormatter.printHelp("java -cp <..> "+getClass().getName()+" <options>","Options:",options,"");
		}


		private Options getOptions() {
				Options options = new Options();
				Option jdbcHost = new Option("j","jdbc-host",true,"The Host running the JDBC client");
				jdbcHost.setRequired(true);
				options.addOption(jdbcHost);
				Option jdbcPort = new Option("p","jdbc-port",true,"The Port running the JDBC client. Default is 1527");
				jdbcPort.setRequired(false);
				options.addOption(jdbcPort);

				Option userName = new Option("u","user-name",true,"The User name to use when connecting. Defaults to app");
				userName.setRequired(false);
				options.addOption(userName);

				Option password = new Option("w","pwd",true,"The Password to use to connect. Defaults to app");
				password.setRequired(false);
				options.addOption(password);

				Option sourcePath = new Option("i","input",true,"The File/Directory where the files to be imported are located");
				sourcePath.setRequired(true);
				options.addOption(sourcePath);

				Option outputPath = new Option("o","output",true,"The Directory where the files should be written to");
				outputPath.setRequired(true);
				options.addOption(outputPath);

				Option schema = new Option("s","schema", true, "The Schema of the table to import into. Defaults to APP");
				schema.setRequired(false);
				options.addOption(schema);

				Option table = new Option("t","table",true,"The Table to import into");
				table.setRequired(true);
				options.addOption(table);

				Option columnsToImport = new Option("c","import-columns",true,"The Columns that are to be imported.Defaults to all columns");
				columnsToImport.setRequired(false);
				options.addOption(columnsToImport);
				Option colDelimiter = new Option("d","column-delimiter",true,"The Column delimiter to use. Defaults to ,");
				colDelimiter.setRequired(false);
				Option charDelimiter = new Option("q","quote-character",true,"The Quote character to use. Defaults to \"");
				charDelimiter.setRequired(false);
				options.addOption(charDelimiter);
				Option timestampFormat = new Option("TS","timestamp-format",true,"The Timestamp format to use. Defaults to yyyy-MM-dd HH:mm:ss");
				timestampFormat.setRequired(false);
				options.addOption(timestampFormat);
				Option timeFormat = new Option("T","time-format",true,"The Time format to use. Defaults to HH:mm:ss");
				timeFormat.setRequired(false);
				options.addOption(timeFormat);
				Option dateFormat = new Option("D","date-format",true,"The Date format to use. Defaults to yyyy-MM-dd");
				dateFormat.setRequired(false);
				options.addOption(dateFormat);

				Option reducers = new Option("r","reduce-tasks",true,"The number of reduce tasks to use. Default is 2");
				reducers.setRequired(false);
				options.addOption(reducers);
				return options;
		}
}
