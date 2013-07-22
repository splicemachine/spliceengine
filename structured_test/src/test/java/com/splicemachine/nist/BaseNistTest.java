package com.splicemachine.nist;

import java.io.*;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.derby.tools.ij;

import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.splicemachine.derby.nist.DerbyEmbedConnection;

public class BaseNistTest {
    public static List<String> SCHEMA_SCRIPTS = new ArrayList<String>();
	public static List<String> SKIP_TESTS = new ArrayList<String>();
    public static List<String> nonSqlFilesFilter;


    public static void loadSkipTests(String fileName, String commentPattern) {
        for (String baseName :  fileToLines(fileName, commentPattern)) {
            SKIP_TESTS.add(baseName + ".sql");
        }
    }

    public static void loadSchemaList(String fileName, String commentPattern) {
        for (String baseName :  fileToLines(fileName, commentPattern)) {
            SKIP_TESTS.add(baseName + ".sql");
        }
    }

	public static class SpliceIOFileFilter implements IOFileFilter {
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

    public static List<String> fileToLines(String filename, String commentPattern) {
        List<String> lines = new LinkedList<String>();
        String line;
        try {
            BufferedReader in = new BufferedReader(new FileReader(filename));
            while ((line = in.readLine()) != null) {
                if (commentPattern != null && ! line.startsWith(commentPattern)) {
                    lines.add(line);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lines;
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
