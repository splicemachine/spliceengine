package com.ir.hive.test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.HadoopTestCase;

public abstract class HiveTestBase extends HadoopTestCase {

	  protected static final Path ROOT_DIR = new Path("testing");

	  public HiveTestBase() throws IOException {
	    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 1, 1);

	    try {
	      Thread.sleep(1000);
	    } catch (InterruptedException e) {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
	    }
	    Map<String, String> env = new HashMap<String, String>();
	    env.putAll(System.getenv());
	    if (System.getenv("HADOOP_HOME") == null) {
	      String shome = System.getProperty("user.home");
	      if (shome != null) {
	        File home = new File(shome);
	        File hadoopR = new File(home, "hadoop");
	        File hadoopHome = new File(hadoopR, "hadoop-0.20.2_local");
	        if (hadoopHome.exists()) {
	          env.put("HADOOP_HOME", hadoopHome.getAbsolutePath());
	          EnvironmentHack.setEnv(env);
	        }
		File target = new File("target/hadoop-0.20.2");
	        if ( target.exists() ){
	          env.put("HADOOP_HOME", target.getAbsolutePath());
	          EnvironmentHack.setEnv(env);
	        }
	      }
	    }
	  }

	  protected Path getDir(Path dir) {
	    if (isLocalFS()) {
	      String localPathRoot = System.getProperty("test.build.data", "/tmp").replace(' ', '+');
	      dir = new Path(localPathRoot, dir);
	    }
	    return dir;
	  }

	  public void setUp() throws Exception {
	    super.setUp();

	    String jarFile = org.apache.hadoop.hive.ql.exec.MapRedTask.class.getProtectionDomain().getCodeSource().getLocation().getFile();
	    System.setProperty(HiveConf.ConfVars.HIVEJAR.toString(), jarFile);

	    Path rootDir = getDir(ROOT_DIR);
	    Configuration conf = createJobConf();
	    FileSystem fs = FileSystem.get(conf);
	    fs.delete(rootDir, true);
	    Path metastorePath = new Path("/tmp/metastore_db");
	    fs.delete(metastorePath, true);
	    Path warehouse = new Path("/tmp/warehouse");
	    fs.delete(warehouse, true);
	    fs.mkdirs(warehouse);
	  }
	}