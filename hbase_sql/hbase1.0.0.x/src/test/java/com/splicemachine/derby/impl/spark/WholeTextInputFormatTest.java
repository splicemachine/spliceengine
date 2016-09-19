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

package com.splicemachine.derby.impl.spark;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author Scott Fines
 *         Date: 8/4/16
 */
public class WholeTextInputFormatTest{

    @Test
    public void testGetsStreamForDirectory() throws Exception{
        /*
         * This test failed before changes to WholeTextInputFormat(hooray for test-driven development!),
         * so this constitutes an effective regression test for SPLICE-739. Of course, we'll be certain
         * about it by ALSO writing an IT, but this is a nice little Unit test of the same thing.
         */
        Configuration configuration =HConfiguration.unwrapDelegate();
        String dirPath =SpliceUnitTest.getResourceDirectory()+"multiLineDirectory";
        configuration.set("mapred.input.dir",dirPath);

        WholeTextInputFormat wtif = new WholeTextInputFormat();
        wtif.setConf(configuration);

        JobContext ctx = new JobContextImpl(configuration,new JobID("test",1));
        List<InputSplit> splits=wtif.getSplits(ctx);

        int i=0;
        Set<String> files = readFileNames(dirPath);

        Set<String> readFiles = new HashSet<>();
        for(InputSplit is:splits){
            TaskAttemptContext tac = new TaskAttemptContextImpl(configuration,new TaskAttemptID("test",1,true,i,1));
            RecordReader<String, InputStream> recordReader=wtif.createRecordReader(is,tac);
            CombineFileSplit cfs = (CombineFileSplit)is;
            System.out.println(cfs);

            long c = collectRecords(readFiles,recordReader);
            Assert.assertEquals("did not read all data!",28,c);
            i++;
        }

        Assert.assertEquals("Did not read all files!",files.size(),readFiles.size());
        for(String expectedFile:files){
            Assert.assertTrue("Did not read file <"+expectedFile+">",readFiles.contains(expectedFile));
        }
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private Set<String> readFileNames(String dirPath){
        Set<String> fileNames = new HashSet<>();
        File d = new File(dirPath);
        //we can assume that d exists, but let's be safe
        Assert.assertTrue("Programmer error: missing directory <"+dirPath+">",d.exists());
        Assert.assertTrue("Programmer error: not a directory <"+dirPath+">",d.isDirectory());
        File[] files=d.listFiles();
        Assert.assertNotNull("Programmer error: did not list files properly <"+dirPath+">",files);
        for(File file:files){
            //CombineFileSplit prepends the file: to the front of all of its paths, so we need to do the same for equality checking
            fileNames.add("file:"+file.getAbsolutePath());
        }
        return fileNames;
    }

    private long collectRecords(Set<String> fileNames,RecordReader<String, InputStream> recordReader) throws IOException, InterruptedException{
        long count = 0L;
        while(recordReader.nextKeyValue()){
            String key = recordReader.getCurrentKey();
            Assert.assertTrue("Seen the same file twice!",fileNames.add(key));

            InputStream is = recordReader.getCurrentValue();
            try(BufferedReader br = new BufferedReader(new InputStreamReader(is))){
                String n;
                while((n = br.readLine())!=null){
                   count++;
                }
            }
        }
        return count;
    }
}