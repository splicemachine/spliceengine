package com.splicemachine.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

import com.splicemachine.utils.Pair;

public class MockSnapshot {
	public static String MOCK_HBASE_ROOT_DIR = "file:/tmp/hbase"; 
	public static String MOCK_HFILE1 = 
			"fam_a/TABLE_A=fa11cc78b20de46c0ee0c63d9075dfeb-f4cf2e414ae54a29aa91315b00b4241e.0c83b84690675f84d4b0659ae09239dc";
	
	public static String MOCK_PATH1 = MOCK_HBASE_ROOT_DIR+ Path.SEPARATOR+ 
			"data/default/TABLE_A/fa11cc78b20de46c0ee0c63d9075dfeb/fam_a/f4cf2e414ae54a29aa91315b00b4241e.0c83b84690675f84d4b0659ae09239dc";
	
// TODO - this is compatible only with CDH5.x
//    public static SnapshotFileInfoOrBuilder getMockSnapshotFileInfo(String hfile) throws IOException {
//    	SnapshotFileInfoOrBuilder testSnapshotInfo = mock(SnapshotFileInfoOrBuilder.class);
//        
//        when(testSnapshotInfo.getType()).thenReturn(SnapshotFileInfo.Type.HFILE);
//        when(testSnapshotInfo.getHfile()).thenReturn(hfile);        
//        return testSnapshotInfo;
//    }
    
    public static Configuration mockHBaseConfiguration(List<Pair<String, String>> attr)
    {
    	Configuration conf = HBaseConfiguration.create();
    	for(Pair<String, String> p: attr){
    		conf.set(p.getFirst(), p.getSecond());
    	}
    	
    	return conf;
    }

    public static void createFile(Path p) throws IOException
    {
    	Configuration conf = new Configuration();
    	conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///tmp");
    	FileSystem fs = FileSystem.getLocal(conf);
    	
    	FSDataOutputStream dos = fs.create(p, true);
    	dos.write(0);
    	dos.flush();
    	dos.close();   	
    }
    
    public static void deleteFile(Path p) throws IOException
    {
    	Configuration conf = new Configuration();
    	conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///tmp");
    	FileSystem fs = p.getFileSystem(conf);
    	fs.delete(p, true);
    }
    
}
