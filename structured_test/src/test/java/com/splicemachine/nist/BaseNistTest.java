package com.splicemachine.nist;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.derby.tools.ij;

import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.splicemachine.derby.nist.DerbyEmbedConnection;

public class BaseNistTest {
	public static List<String> SKIP_TESTS = Arrays.asList(
//	#cdr007 - bug 555
//	#Added order by these failing tests due to mismatch.
//	# basetabs,dml001,dml012,dml015,dml 016,dml044
//	# dml046,dml058,dml060,dml068,xts730,yts812
//	# Bug 552, 553, 599
	"dml001",
//	# dml014 - bug 625
	"dml014",
//	# dml020: Bug 597 self joins
	"dml020",
//	# dml022: bug 492
	"dml022",
//	# dml023: This tests will always fail due to test 0107 where varchar column is compared with blank padded value.
//	#dml023
//	# dml024: bug 495
//	#dml024
//	#dml026 - The only difference is when there is  ERROR message output from query, Derby's output is different from Splice
	"dml026",
//	# dml034: bug384
	"dml034",
//	#dml035 - Bug 560
	"dml035",
//	# dml049: select from 10 tables in where clause
	"dml049",
//	# dml050: bug 492
	"dml050",
//	# dml057: bug 384
	"dml057",
//	# dml061: bug 574, 575
//	#dml069 - Bug 577
//	# Bug 628
	"dml073",
//	# Bug 601
	"dml075",
//	#dml079
//	# dml081 - divide by zero
	"dml081",
//	# dml083 - NULL in column where Max(column)
	"dml083",
	"dml087",
//	# dml090 - Bug 601
	"dml091",
	"dml104",
	"dml106",
	"dml112",
	"dml114",
	"dml119",
	"dml132",
	"dml144",
	"dml147",
	"dml148",
//	#dml149: bug 499
	"dml149",
	"dml155",
	"dml158",
	"dml162",
//	# dml168: bug 500
	"dml168",
//	#dml170: Bug 456, bug 501
	"dml170",
	"xts701",
//	#xts729 - Bug 562
//	#xts742 - Bug 630
	"xts742",
//	# xts752: alter table add constraint
	"xts752",
	"yts797",
	"yts798",
//	#yts811 - Bug 592
	"yts811",
//	# yts799: bug 494
	"yts799",
	"drop");
	
	protected static List<String> SCHEMA_SCRIPTS = Arrays.asList(
			"schema1.sql",
			"temp_schem10.sql",
			"temp_cts5sch2.sql",
			"schema5.sql",
			"schema8.sql",
			"temp_cts5tab.sql",
			"schema4.sql");
	
	protected class SpliceIOFileFilter implements IOFileFilter {
		private List<String> inclusions;
		private List<String> exclusions;
		public SpliceIOFileFilter(List<String> inclusions, List<String> exclusions) {
			this.inclusions = inclusions;
			this.exclusions = exclusions;
		}

		@Override
		public boolean accept(File file) {
			if (inclusions != null) {
				if (inclusions.contains(file.getName()))
					return true;
				else 
					return false;
			}
			if (exclusions != null) {
				if (exclusions.contains(file.getName()))
					return false;
			}
			return true;
		}

		@Override
		public boolean accept(File dir, String name) {
			// TODO Auto-generated method stub
			return false;
		}
		
	}
	public static String getBaseDirectory() {
		String userDir = System.getProperty("user.dir");
	    if(!userDir.endsWith("structured_test"))
	    	userDir = userDir+"/structured_test/";
	    return userDir;
	}
	public static String getResourceDirectory() {
		return getBaseDirectory()+"/src/test/resources/";
	}

	public static void runTest(File file, String type, Connection connection) throws Exception {
		FileInputStream fis = null;
		FileOutputStream fop = null;
		try {
			fis = new FileInputStream(file);
			File targetFile = new File(getBaseDirectory()+"/target/nist/" + file.getName().replace(".sql", type));
			Files.createParentDirs(targetFile);
			if (targetFile.exists())
				targetFile.delete();
			targetFile.createNewFile();
			fop = new FileOutputStream(targetFile);
			ij.runScript(connection, fis,"UTF-8",fop,"UTF-8");		
			fop.flush();
		} catch (Exception e) {
			throw e;
		} finally {
			Closeables.closeQuietly(fop);
			Closeables.closeQuietly(fis);
		}
	}


}
