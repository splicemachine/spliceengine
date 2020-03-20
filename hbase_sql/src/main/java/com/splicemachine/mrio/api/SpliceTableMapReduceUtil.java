/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

/**
 * SpliceTableMapReduceUtil which wraps up params for Map Reduce Job.
 *
 * @author Yanan Jian
 * Created on: 08/14/14
 */
package com.splicemachine.mrio.api;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.SMSQLUtil;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HRegionPartitioner;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.regionserver.HBasePlatformUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLDecoder;
import java.sql.SQLException;
import java.util.*;

@SuppressWarnings("unchecked")
public class SpliceTableMapReduceUtil{
    static Log LOG=LogFactory.getLog(SpliceTableMapReduceUtil.class);
    private static SMSQLUtil sqlUtil=null;

    /**
     * Use this before submitting a TableMap job. It will appropriately set up
     * the job.
     *
     * @param table  The Splice table name to read from.
     * @param scan  The scan instance with the columns, time range etc.
     * @param mapper  The mapper class to use.
     * @param outputKeyClass  The class of the output key.
     * @param outputValueClass  The class of the output value.
     * @param job  The current job to adjust.  Make sure the passed job is
     * carrying all necessary HBase configuration.
     * @throws IOException When setting up the details fails.
     */
    public static void initTableMapperJob(String table,Scan scan,
                                          Class<? extends Mapper> mapper,
                                          Class<? extends WritableComparable> outputKeyClass,
                                          Class<? extends Object> outputValueClass,Job job)
            throws IOException{
        initTableMapperJob(table,scan,mapper,outputKeyClass,outputValueClass,
                job,true);
    }


    /**
     * Use this before submitting a TableMap job. It will appropriately set up
     * the job.
     *
     * @param table Binary representation of the Splice table name to read from.
     * @param scan  The scan instance with the columns, time range etc.
     * @param mapper  The mapper class to use.
     * @param outputKeyClass  The class of the output key.
     * @param outputValueClass  The class of the output value.
     * @param job  The current job to adjust.  Make sure the passed job is
     * carrying all necessary HBase configuration.
     * @throws IOException When setting up the details fails.
     */
    public static void initTableMapperJob(byte[] table,Scan scan,
                                          Class<? extends Mapper> mapper,
                                          Class<? extends WritableComparable> outputKeyClass,
                                          Class<? extends Object> outputValueClass,Job job)
            throws IOException{
        initTableMapperJob(Bytes.toString(table),scan,mapper,outputKeyClass,outputValueClass,
                job,true);
    }

    /**
     * Use this before submitting a TableMap job. It will appropriately set up
     * the job.
     *
     * @param table  The Splice table name to read from.
     * @param scan  The scan instance with the columns, time range etc.
     * @param mapper  The mapper class to use.
     * @param outputKeyClass  The class of the output key.
     * @param outputValueClass  The class of the output value.
     * @param job  The current job to adjust.  Make sure the passed job is
     * carrying all necessary HBase configuration.
     * @param addDependencyJars upload HBase jars and jars for any of the configured
     *           job classes via the distributed cache (tmpjars).
     * @throws IOException When setting up the details fails.
     */
    public static void initTableMapperJob(String table,Scan scan,
                                          Class<? extends Mapper> mapper,
                                          Class<? extends WritableComparable> outputKeyClass,
                                          Class<? extends Object> outputValueClass,Job job,
                                          boolean addDependencyJars,Class<? extends InputFormat> inputFormatClass)
            throws IOException{
        job.setInputFormatClass(inputFormatClass);
        if(outputValueClass!=null) job.setMapOutputValueClass(outputValueClass);
        if(outputKeyClass!=null) job.setMapOutputKeyClass(outputKeyClass);
        if(mapper!=null) job.setMapperClass(mapper);
        job.getConfiguration().set(MRConstants.SPLICE_INPUT_TABLE_NAME,table);
        job.getConfiguration().set(TableInputFormat.SCAN,convertScanToString(scan));
        if(addDependencyJars){
            addDependencyJars(job);
        }

    }

    /**
     * Use this before submitting a TableMap job. It will appropriately set up
     * the job.
     *
     * @param table Binary representation of the Splice table name to read from.
     * @param scan  The scan instance with the columns, time range etc.
     * @param mapper  The mapper class to use.
     * @param outputKeyClass  The class of the output key.
     * @param outputValueClass  The class of the output value.
     * @param job  The current job to adjust.  Make sure the passed job is
     * carrying all necessary HBase configuration.
     * @param addDependencyJars upload HBase jars and jars for any of the configured
     *           job classes via the distributed cache (tmpjars).
     * @param inputFormatClass The class of the input format
     * @throws IOException When setting up the details fails.
     */
    public static void initTableMapperJob(byte[] table,Scan scan,
                                          Class<? extends Mapper> mapper,
                                          Class<? extends WritableComparable> outputKeyClass,
                                          Class<? extends Object> outputValueClass,Job job,
                                          boolean addDependencyJars,Class<? extends InputFormat> inputFormatClass)
            throws IOException{
        initTableMapperJob(Bytes.toString(table),scan,mapper,outputKeyClass,
                outputValueClass,job,addDependencyJars,inputFormatClass);
    }

    /**
     * Use this before submitting a TableMap job. It will appropriately set up
     * the job.
     *
     * @param table Binary representation of the Splice table name to read from.
     * @param scan  The scan instance with the columns, time range etc.
     * @param mapper  The mapper class to use.
     * @param outputKeyClass  The class of the output key.
     * @param outputValueClass  The class of the output value.
     * @param job  The current job to adjust.  Make sure the passed job is
     * carrying all necessary HBase configuration.
     * @param addDependencyJars upload HBase jars and jars for any of the configured
     *           job classes via the distributed cache (tmpjars).
     * @throws IOException When setting up the details fails.
     */
    public static void initTableMapperJob(byte[] table,Scan scan,
                                          Class<? extends Mapper> mapper,
                                          Class<? extends WritableComparable> outputKeyClass,
                                          Class<? extends Object> outputValueClass,Job job,
                                          boolean addDependencyJars)
            throws IOException{
        initTableMapperJob(Bytes.toString(table),scan,mapper,outputKeyClass,
                outputValueClass,job,addDependencyJars,TableInputFormat.class);
    }

    /**
     * Use this before submitting a TableMap job. It will appropriately set up
     * the job.
     *
     * @param table The Splice table name to read from.
     * @param scan  The scan instance with the columns, time range etc.
     * @param mapper  The mapper class to use.
     * @param outputKeyClass  The class of the output key.
     * @param outputValueClass  The class of the output value.
     * @param job  The current job to adjust.  Make sure the passed job is
     * carrying all necessary HBase configuration.
     * @param addDependencyJars upload HBase jars and jars for any of the configured
     *           job classes via the distributed cache (tmpjars).
     * @throws IOException When setting up the details fails.
     */
    public static void initTableMapperJob(String table,Scan scan,
                                          Class<? extends Mapper> mapper,
                                          Class<? extends WritableComparable> outputKeyClass,
                                          Class<? extends Object> outputValueClass,Job job,
                                          boolean addDependencyJars)
            throws IOException{
        initTableMapperJob(table,scan,mapper,outputKeyClass,
                outputValueClass,job,addDependencyJars,TableInputFormat.class);
    }

    /**
     * Writes the given scan into a Base64 encoded string.
     *
     * @param scan  The scan to write out.
     * @return The scan saved in a Base64 encoded string.
     * @throws IOException When writing the scan fails.
     */
    public static String convertScanToString(Scan scan) throws IOException{
        ObjectOutput dos=null;
        try{
            byte[] bytes=ProtobufUtil.toScan(scan).toByteArray();
            return Base64.getEncoder().encodeToString(bytes);
        }finally{
            if(dos!=null)
                dos.close();
        }
    }

    /**
     * Converts the given Base64 string back into a Scan instance.
     *
     * @param base64  The scan details.
     * @return The newly created Scan instance.
     * @throws IOException When reading the scan instance fails.
     */
    public static Scan convertStringToScan(String base64) throws IOException{
        byte[] bytes= Base64.getDecoder().decode(base64);
        ClientProtos.Scan scan=ClientProtos.Scan.parseFrom(bytes);
        return ProtobufUtil.toScan(scan);
    }

    /**
     * Use this before submitting a TableReduce job. It will
     * appropriately set up the JobConf.
     *
     * @param table  The Splice output table.
     * @param reducer  The reducer class to use.
     * @param job  The current job to adjust.
     * @throws IOException When determining the region count fails.
     * @throws SQLException
     */
    public static void initTableReducerJob(String table,
                                           Class<? extends Reducer> reducer,Job job)
            throws IOException, SQLException{
        initTableReducerJob(table,reducer,job,null);
    }

    /**
     * Use this before submitting a TableReduce job. It will
     * appropriately set up the JobConf.
     *
     * @param table  The Splice output table.
     * @param reducer  The reducer class to use.
     * @param job  The current job to adjust.
     * @param partitioner  Partitioner to use. Pass <code>null</code> to use
     * default partitioner.
     * @throws IOException When determining the region count fails.
     * @throws SQLException
     */
    public static void initTableReducerJob(String table,
                                           Class<? extends Reducer> reducer,Job job,
                                           Class partitioner) throws IOException, SQLException{
        initTableReducerJob(table,reducer,job,partitioner,null,null,null);
    }

    /**
     * Use this before submitting a TableReduce job. It will
     * appropriately set up the JobConf.
     *
     * @param table  The Splice output table.
     * @param reducer  The reducer class to use.
     * @param job  The current job to adjust.  Make sure the passed job is
     * carrying all necessary HBase configuration.
     * @param partitioner  Partitioner to use. Pass <code>null</code> to use
     * default partitioner.
     * @param quorumAddress Distant cluster to write to; default is null for
     * output to the cluster that is designated in <code>hbase-site.xml</code>.
     * Set this String to the zookeeper ensemble of an alternate remote cluster
     * when you would have the reduce write a cluster that is other than the
     * default; e.g. copying tables between clusters, the source would be
     * designated by <code>hbase-site.xml</code> and this param would have the
     * ensemble address of the remote cluster.  The format to pass is particular.
     * Pass <code> &lt;hbase.zookeeper.quorum>:&lt;hbase.zookeeper.client.port>:&lt;zookeeper.znode.parent>
     * </code> such as <code>server,server2,server3:2181:/hbase</code>.
     * @param serverClass redefined hbase.regionserver.class
     * @param serverImpl redefined hbase.regionserver.client
     * @throws IOException When determining the region count fails.
     * @throws SQLException
     */
    public static void initTableReducerJob(String table,
                                           Class<? extends Reducer> reducer,Job job,
                                           Class partitioner,String quorumAddress,String serverClass,
                                           String serverImpl) throws IOException, SQLException{
        initTableReducerJob(table,reducer,job,partitioner,quorumAddress,
                serverClass,serverImpl,true,TableOutputFormat.class);
    }

    /**
     * Use this before submitting a TableReduce job. It will
     * appropriately set up the JobConf.
     *
     * @param table  The output Splice table name, The format should be Schema.tableName.
     * @param reducer  The reducer class to use.
     * @param job  The current job to adjust.  Make sure the passed job is
     * carrying all necessary configuration.
     * @param partitioner  Partitioner to use. Pass <code>null</code> to use
     * default partitioner.
     * @param quorumAddress Distant cluster to write to; default is null for
     * output to the cluster that is designated in <code>hbase-site.xml</code>.
     * Set this String to the zookeeper ensemble of an alternate remote cluster
     * when you would have the reduce write a cluster that is other than the
     * default; e.g. copying tables between clusters, the source would be
     * designated by <code>hbase-site.xml</code> and this param would have the
     * ensemble address of the remote cluster.  The format to pass is particular.
     * Pass <code> &lt;hbase.zookeeper.quorum>:&lt;hbase.zookeeper.client.port>:&lt;zookeeper.znode.parent>
     * </code> such as <code>server,server2,server3:2181:/hbase</code>.
     * @param serverClass redefined hbase.regionserver.class
     * @param serverImpl redefined hbase.regionserver.client
     * @param addDependencyJars upload HBase jars and jars for any of the configured
     *           job classes via the distributed cache (tmpjars).
     * @throws IOException When determining the region count fails.
     * @throws SQLException
     */
    public static void initTableReducerJob(String table,
                                           Class<? extends Reducer> reducer,Job job,
                                           Class partitioner,
                                           String quorumAddress,
                                           String serverClass,
                                           String serverImpl,boolean addDependencyJars,Class<? extends OutputFormat> outputformatClass) throws IOException{

        Configuration conf=job.getConfiguration();
        job.setOutputFormatClass(outputformatClass);
        if(reducer!=null) job.setReducerClass(reducer);
        conf.set(MRConstants.SPLICE_OUTPUT_TABLE_NAME,table);
        if(sqlUtil==null)
            sqlUtil=SMSQLUtil.getInstance(conf.get(MRConstants.SPLICE_JDBC_STR));
        // If passed a quorum/ensemble address, pass it on to TableOutputFormat.
        String hbaseTableID=null;
        try{
            hbaseTableID=sqlUtil.getConglomID(table);
        }catch(SQLException e){
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw new IOException(e);
        }
        conf.set(MRConstants.HBASE_OUTPUT_TABLE_NAME,table);

        if(quorumAddress!=null){
            // Calling this will validate the format
            ZKConfig.validateClusterKey(quorumAddress);
            conf.set(TableOutputFormat.QUORUM_ADDRESS,quorumAddress);
        }
        if(serverClass!=null && serverImpl!=null){
            conf.set(TableOutputFormat.REGION_SERVER_CLASS,serverClass);
            conf.set(TableOutputFormat.REGION_SERVER_IMPL,serverImpl);

        }
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Object.class);
        if(partitioner==HRegionPartitioner.class){
            job.setPartitionerClass(HRegionPartitioner.class);
            // TODO Where are the keys?
            int regions=getReduceNumberOfRegions(hbaseTableID);
            if(job.getNumReduceTasks()>regions){
                job.setNumReduceTasks(regions);
            }
        }else if(partitioner!=null){
            job.setPartitionerClass(partitioner);
        }

        if(addDependencyJars){
            addDependencyJars(job);
        }

        //initCredentials(job);
    }

    protected static int getReduceNumberOfRegions(String hbaseTableID) throws IOException{
        SConfiguration configuration=SIDriver.driver().getConfiguration();
        Connection connection=HBaseConnectionFactory.getInstance(configuration).getConnection();
        int regions;
        TableName tn = HBaseTableInfoFactory.getInstance(configuration).getTableInfo(hbaseTableID);
        try(RegionLocator rl = connection.getRegionLocator(tn)){
            List<HRegionLocation> allRegionLocations=rl.getAllRegionLocations();
            regions = allRegionLocations.size();
        }
        return regions;
    }

    /**
     * Ensures that the given number of reduce tasks for the given job
     * configuration does not exceed the number of regions for the given table.
     *
     * @param table  The Splice table to get the region count for.
     * @param job  The current job to adjust.
     * @throws IOException When retrieving the table details fails.
     * @throws SQLException When Splice retrieving conglom ID fails.
     */
    public static void limitNumReduceTasks(String table,Job job)
            throws IOException, SQLException{
        Configuration conf=job.getConfiguration();
        if(sqlUtil==null)
            sqlUtil=SMSQLUtil.getInstance(conf.get(MRConstants.SPLICE_JDBC_STR));
        // If passed a quorum/ensemble address, pass it on to TableOutputFormat.
        String hbaseTableID=sqlUtil.getConglomID(table);
        int regions=getReduceNumberOfRegions(hbaseTableID);
        if(job.getNumReduceTasks()>regions)
            job.setNumReduceTasks(regions);
    }

    /**
     * Sets the number of reduce tasks for the given job configuration to the
     * number of regions the given table has.
     *
     * @param table  The Splice table to get the region count for.
     * @param job  The current job to adjust.
     * @throws IOException When retrieving the table details fails.
     * @throws SQLException When Splice retrieving conglom ID fails.
     */
    public static void setNumReduceTasks(String table,Job job)
            throws IOException, SQLException{
        Configuration conf=job.getConfiguration();
        if(sqlUtil==null)
            sqlUtil=SMSQLUtil.getInstance(conf.get(MRConstants.SPLICE_JDBC_STR));
        // If passed a quorum/ensemble address, pass it on to TableOutputFormat.
        String hbaseTableID=sqlUtil.getConglomID(table);
        int regions=getReduceNumberOfRegions(hbaseTableID);
        job.setNumReduceTasks(regions);
    }

    /**
     * Sets the number of rows to return and cache with each scanner iteration.
     * Higher caching values will enable faster mapreduce jobs at the expense of
     * requiring more heap to contain the cached rows.
     *
     * @param job The current job to adjust.
     * @param batchSize The number of rows to return in batch with each scanner
     * iteration.
     */
    public static void setScannerCaching(Job job,int batchSize){
        job.getConfiguration().setInt("hbase.client.scanner.caching",batchSize);
    }

    /**
     * Add the dependency jars as well as jars for any of the configured
     * job classes to the job configuration, so that JobClient will ship them
     * to the cluster and add them to the DistributedCache.
     */
    public static void addDependencyJars(Job job) throws IOException{
        try{
            addDependencyJars(job.getConfiguration(),
                    org.apache.zookeeper.ZooKeeper.class,
                    com.google.protobuf.Message.class,
                    com.google.common.collect.ImmutableSet.class,
                    job.getMapOutputKeyClass(),
                    job.getMapOutputValueClass(),
                    job.getInputFormatClass(),
                    job.getOutputKeyClass(),
                    job.getOutputValueClass(),
                    job.getOutputFormatClass(),
                    job.getPartitionerClass(),
                    job.getCombinerClass());
        }catch(ClassNotFoundException e){
            throw new IOException(e);
        }
    }

    /**
     * Add the jars containing the given classes to the job's configuration
     * such that JobClient will ship them to the cluster and add them to
     * the DistributedCache.
     */
    public static void addDependencyJars(Configuration conf,
                                         Class... classes) throws IOException{

        FileSystem localFs=FileSystem.getLocal(conf);

        Set<String> jars=new HashSet<String>(conf.getStringCollection("tmpjars"));

        // Add jars that are already in the tmpjars variable

        // Add jars containing the specified classes
        for(Class clazz : classes){
            if(clazz==null) continue;

            String pathStr=findOrCreateJar(clazz);
            if(pathStr==null){
                LOG.warn("Could not find jar for class "+clazz+
                        " in order to ship it to the cluster.");
                continue;
            }
            Path path=new Path(pathStr);
            if(!localFs.exists(path)){
                LOG.warn("Could not validate jar file "+path+" for class "
                        +clazz);
                continue;
            }
            jars.add(path.makeQualified(localFs.getUri(), localFs.getWorkingDirectory()).toString());
        }
        if(jars.isEmpty()) return;

        conf.set("tmpjars",
                StringUtils.arrayToString(jars.toArray(new String[0])));
    }

    /**
     * If org.apache.hadoop.util.JarFinder is available (0.23+ hadoop),
     * finds the Jar for a class or creates it if it doesn't exist. If
     * the class is in a directory in the classpath, it creates a Jar
     * on the fly with the contents of the directory and returns the path
     * to that Jar. If a Jar is created, it is created in
     * the system temporary directory.
     *
     * Otherwise, returns an existing jar that contains a class of the
     * same name.
     *
     * @param my_class the class to find.
     * @return a jar file that contains the class, or null.
     * @throws IOException
     */
    private static String findOrCreateJar(Class my_class)
            throws IOException{
        try{
            Class<?> jarFinder=Class.forName("org.apache.hadoop.util.JarFinder");
            // hadoop-0.23 has a JarFinder class that will create the jar
            // if it doesn't exist.  Note that this is needed to run the mapreduce
            // unit tests post-0.23, because mapreduce v2 requires the relevant jars
            // to be in the mr cluster to do output, split, etc.  At unit test time,
            // the hbase jars do not exist, so we need to create some.  Note that we
            // can safely fall back to findContainingJars for pre-0.23 mapreduce.
            Method m=jarFinder.getMethod("getJar",Class.class);
            return (String)m.invoke(null,my_class);
        }catch(InvocationTargetException ite){
            // function was properly called, but threw it's own exception
            throw new IOException(ite.getCause());
        }catch(Exception e){
            // ignore all other exceptions. related to reflection failure
        }

        LOG.debug("New JarFinder: org.apache.hadoop.util.JarFinder.getJar "+
                "not available.  Using old findContainingJar");
        return findContainingJar(my_class);
    }

    /**
     * Find a jar that contains a class of the same name, if any.
     * It will return a jar file, even if that is not the first thing
     * on the class path that has a class with the same name.
     *
     * This is shamelessly copied from JobConf
     *
     * @param my_class the class to find.
     * @return a jar file that contains the class, or null.
     * @throws IOException
     */
    private static String findContainingJar(Class my_class){
        ClassLoader loader=my_class.getClassLoader();
        String class_file=my_class.getName().replaceAll("\\.","/")+".class";
        try{
            for(Enumeration itr=loader.getResources(class_file);
                itr.hasMoreElements();){
                URL url=(URL)itr.nextElement();
                if("jar".equals(url.getProtocol())){
                    String toReturn=url.getPath();
                    if(toReturn.startsWith("file:")){
                        toReturn=toReturn.substring("file:".length());
                    }
                    // URLDecoder is a misnamed class, since it actually decodes
                    // x-www-form-urlencoded MIME type rather than actual
                    // URL encoding (which the file path has). Therefore it would
                    // decode +s to ' 's which is incorrect (spaces are actually
                    // either unencoded or encoded as "%20"). Replace +s first, so
                    // that they are kept sacred during the decoding process.
                    toReturn=toReturn.replaceAll("\\+","%2B");
                    toReturn=URLDecoder.decode(toReturn,"UTF-8");
                    return toReturn.replaceAll("!.*$","");
                }
            }
        }catch(IOException e){
            throw new RuntimeException(e);
        }
        return null;
    }


}
