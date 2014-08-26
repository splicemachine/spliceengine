SpliceMR
========

Map Reduce Integration with Splice DB

###Usage of SpliceInputFormat:
#### Preparation
1. define table name that you want to retrieve data from (That table should exist in Splice Machine DB)
> String inputTableName = "WIKIDATA";<br>
  String outputTableName = "USERTEST"<br>
  //table names can be specified as schemaName.tableName<br>
  //if given the tableName as WIKIDATA, then it will use the default schema.

2. new instance of Job
> Configuration config = HBaseConfiguration.create();
> Job job = new Job(config, "MRTest2");

3. init mapper job
> TableMapReduceUtil.initTableMapperJob(
			    tableName, &nbsp;&nbsp;&nbsp;&nbsp;// input Splice table name<br>
			    scan, &nbsp;&nbsp;&nbsp;&nbsp;// Scan instance to control CF and attribute selection<br>
			    MyMapper.class, &nbsp;&nbsp;&nbsp;&nbsp;// mapper<br>
			    Text.class, &nbsp;&nbsp;&nbsp;&nbsp;// mapper output key<br>
			    IntWritable.class,  &nbsp;&nbsp;&nbsp;&nbsp;// mapper output value<br>
			    job,<br>
			    true,<br>
			    SpliceInputFormat.class);<br>

#### Retrieve value within map function
1. implement your map function
> public void map(ImmutableBytesWritable row, Result value, Context context)
{
&nbsp;&nbsp;&nbsp;&nbsp;// use 'value' to retreive a single row. 
}

2. retreive and parse single row with specified columns
> DataValueDescriptor dvd[] = value.getRowArray();

3. print it out
> see the WordCount.decode();

#### Manipulate and Save value within reduce function
1. create ExecRow <br>
2. fill in the row with value: execRow.setRowArray(DataValueDescriptors...)<br>

#### Commit your job if it is successful
> job.commit()

#### Rollback your job if it fails
> job.rollback()

#### Run the example
1. start your hdfs, yarn or hadoop
> $ ./sbin/start-all.sh

2. run ImportData.java
> add some args to this script, for example: WIKIDATA &nbsp;&nbsp;&nbsp;&nbsp;'create table WIKIDATA(WORD varchar(100))'&nbsp;&nbsp;&nbsp;/src/main/java/com/splicemachine/mrio/sample/mediawiki.dic.txt
> this will load data in mediawiki.dic.txt into WIKIDATA table in Splice

3. run CreateOutputTable.java
> add some args to this script, for example: USERTEST&nbsp;&nbsp;&nbsp;&nbsp;'create table USERTEST (WORD varchar(100), COUNT integer)'
> this will create the output table for reducer.

4. when the map-reduce finished running, you can checkout the result in Splice, data will be saved in the output table you created. 
