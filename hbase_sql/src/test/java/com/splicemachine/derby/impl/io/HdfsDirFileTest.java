/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import com.splicemachine.access.HBaseConfigurationSource;
import com.splicemachine.access.HConfiguration;
import org.apache.hadoop.fs.FileUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.splicemachine.db.io.StorageFile;

public class HdfsDirFileTest {
	private static String localBaseDir = "target/HdfsDirFileTest";

	/*
	 * ========================================================================
	 * Set-up and Tear-down Methods
	 * ========================================================================
	 */

	@BeforeClass
	public static void beforeClass() throws Exception {
        // necessary for mapr
        HConfiguration.unwrapDelegate().set("fs.default.name", "file:///");

        FileUtil.fullyDelete(new File(localBaseDir).getAbsoluteFile());
	}

	@AfterClass
	public static void afterClass() throws Exception {
	}

	/*
	 * ========================================================================
	 * Helper Methods
	 * ========================================================================
	 */

	/**
	 * Factory method to create a new HdfsDirFile object.
	 * 
	 * @param pathName  name of a file or directory
	 *
	 * @return a new HdfsDirFile object
	 */
	private HdfsDirFile createHdfsDirFile(String pathName) {
		HdfsDirFile file = new HdfsDirFile(localBaseDir, pathName);
		return file;
	}

	/**
	 * Factory method to create a new HdfsDirFile object.
	 * 
	 * @param pathName  name of a file or directory
	 *
	 * @return a new HdfsDirFile object
	 */
	private HdfsDirFile createHdfsDirFile(HdfsDirFile parent, String pathName) {
		HdfsDirFile file = new HdfsDirFile(parent, pathName);
		return file;
	}

	/*
	 * ========================================================================
	 * Test Methods
	 * ========================================================================
	 */

	@Test
	public void testCreateExistDeleteFile() throws IOException {
		HdfsDirFile file = createHdfsDirFile("foo1.txt");
		Assert.assertTrue("File was not created", file.createNewFile());
		Assert.assertTrue("File does not exist", file.exists());
		Assert.assertTrue("File was not deleted", file.delete());
	}

	@Test
	public void testCreateListDeleteDirFiles() throws IOException {
		HdfsDirFile dir = createHdfsDirFile("myfolder2");
		Assert.assertTrue("Directory was not created", dir.mkdir());
		Assert.assertTrue("Directory is not identifying as being a directory", dir.isDirectory());
		HdfsDirFile file1 = createHdfsDirFile(dir, "able2.txt");
		Assert.assertTrue("File was not created", file1.createNewFile());
		HdfsDirFile file2 = createHdfsDirFile(dir, "baker2.txt");
		Assert.assertTrue("File was not created", file2.createNewFile());
		HdfsDirFile file3 = createHdfsDirFile(dir, "charlie2.txt");
		Assert.assertTrue("File was not created", file3.createNewFile());

		String[] files = dir.list();
		Assert.assertNotNull("The list method returned null", files);
		Assert.assertEquals("The list method returned the wrong number of files", 3, files.length);

		Assert.assertTrue("File was not deleted", file1.delete());
		Assert.assertTrue("File was not deleted", file2.delete());
		Assert.assertTrue("File was not deleted", file3.delete());
		Assert.assertTrue("File was not deleted", dir.delete());
	}

	@Test
	public void testDeleteAll() throws IOException {
		HdfsDirFile dir = createHdfsDirFile("myfolder3");
		Assert.assertTrue("Directory was not created", dir.mkdir());
		HdfsDirFile file1 = createHdfsDirFile(dir, "able3.txt");
		Assert.assertTrue("File was not created", file1.createNewFile());
		HdfsDirFile file2 = createHdfsDirFile(dir, "baker3.txt");
		Assert.assertTrue("File was not created", file2.createNewFile());
		HdfsDirFile file3 = createHdfsDirFile(dir, "charlie3.txt");
		Assert.assertTrue("File was not created", file3.createNewFile());

		Assert.assertTrue("File was not deleted", dir.deleteAll());
	}

	@Test
	public void testPathsAndNames() throws IOException {
		HdfsDirFile dir = createHdfsDirFile("myfolder4");
		Assert.assertTrue("Directory was not created", dir.mkdir());
		HdfsDirFile file1 = createHdfsDirFile(dir, "able4.txt");
		Assert.assertTrue("File was not created", file1.createNewFile());

		Assert.assertEquals("Directory path is wrong", localBaseDir + File.separator + "myfolder4", dir.getPath());
		Assert.assertEquals("Directory canonical path is wrong", localBaseDir + File.separator + "myfolder4", dir.getCanonicalPath());
		Assert.assertEquals("Directory name is wrong", "myfolder4", dir.getName());
		Assert.assertEquals("File path is wrong", localBaseDir + File.separator + "myfolder4/able4.txt", file1.getPath());
		Assert.assertEquals("File canonical path is wrong", localBaseDir + File.separator + "myfolder4/able4.txt", file1.getCanonicalPath());
		Assert.assertEquals("File name is wrong", "able4.txt", file1.getName());
	}

	@Test
	public void testMkdirs() throws IOException {
		HdfsDirFile dir = createHdfsDirFile("myfolder5/foo/bar");
		Assert.assertTrue("Directories were not created", dir.mkdirs());
		HdfsDirFile file1 = createHdfsDirFile(dir, "able5.txt");
		Assert.assertTrue("File was not created", file1.createNewFile());
		HdfsDirFile file2 = createHdfsDirFile(dir, "baker5.txt");
		Assert.assertTrue("File was not created", file2.createNewFile());
		HdfsDirFile file3 = createHdfsDirFile(dir, "charlie5.txt");
		Assert.assertTrue("File was not created", file3.createNewFile());

		String[] files = dir.list();
		Assert.assertNotNull("The list method returned null", files);
		Assert.assertEquals("The list method returned the wrong number of files", 3, files.length);
	}

	@Test
	public void testParentDir() throws IOException {
		HdfsDirFile dir = createHdfsDirFile("myfolder6");
		Assert.assertTrue("Directory was not created", dir.mkdir());
		HdfsDirFile file1 = createHdfsDirFile(dir, "able6.txt");

		StorageFile parent = file1.getParentDir();
		Assert.assertNotNull("Parent directory is null", parent);
		Assert.assertEquals("Parent has the wrong path", localBaseDir + File.separator + "myfolder6", parent.getPath());
	}

	@Test
	public void testOutputAndInputStreams() throws IOException {
		HdfsDirFile dir = createHdfsDirFile("myfolder7");
		Assert.assertTrue("Directory was not created", dir.mkdir());
		HdfsDirFile file1 = createHdfsDirFile(dir, "able7.txt");

		String line1 = "This is line 1.";
		String line2 = "This is line 2.";
		String line3 = "This is line 3.";

		PrintWriter writer = new PrintWriter(file1.getOutputStream());
		writer.write(line1 + "\n");
		writer.write(line2 + "\n");
		writer.write(line3 + "\n");
		writer.close();

		BufferedReader reader = new BufferedReader(new InputStreamReader(file1.getInputStream()));
		try {
			Assert.assertEquals("Line 1 from the file does not match", line1, reader.readLine());
			Assert.assertEquals("Line 2 from the file does not match", line2, reader.readLine());
			Assert.assertEquals("Line 3 from the file does not match", line3, reader.readLine());
			Assert.assertNull("Reader is not null (not EOF)", reader.readLine());
		} finally {
			reader.close();
		}
	}

	@Test @Ignore("Append is an optional operation for the FileSystem and is not supported by the LocalFileSystem.")
	public void testOutputStreamAppend() throws IOException {
		HdfsDirFile dir = createHdfsDirFile("myfolder8");
		Assert.assertTrue("Directory was not created", dir.mkdir());
		HdfsDirFile file1 = createHdfsDirFile(dir, "able8.txt");

		String line1 = "This is line 1.";
		String line2 = "This is line 2.";
		String line3 = "This is line 3.";
		String line4 = "This is line 4.";
		String line5 = "This is line 5.";

		// Create and write to the file.
		PrintWriter writer = new PrintWriter(file1.getOutputStream());
		writer.write(line1 + "\n");
		writer.write(line2 + "\n");
		writer.write(line3 + "\n");
		writer.close();

		// Append to the file.
		writer = new PrintWriter(file1.getOutputStream(true));
		writer.write(line4 + "\n");
		writer.write(line5 + "\n");
		writer.close();

		// Check the contents of the file.
		BufferedReader reader = new BufferedReader(new InputStreamReader(file1.getInputStream()));
		try {
			Assert.assertEquals("Line 1 from the file does not match", line1, reader.readLine());
			Assert.assertEquals("Line 2 from the file does not match", line2, reader.readLine());
			Assert.assertEquals("Line 3 from the file does not match", line3, reader.readLine());
			Assert.assertEquals("Line 4 from the file does not match", line4, reader.readLine());
			Assert.assertEquals("Line 5 from the file does not match", line5, reader.readLine());
			Assert.assertNull("Reader is not null (not EOF)", reader.readLine());
		} finally {
			reader.close();
		}
	}

	@Test
	public void testRename() throws IOException {
		HdfsDirFile dir = createHdfsDirFile("myfolder9");
		Assert.assertTrue("Directory was not created", dir.mkdir());
		HdfsDirFile file1 = createHdfsDirFile(dir, "able9.txt");
		file1.createNewFile();
		HdfsDirFile file2 = createHdfsDirFile(dir, "baker9.txt");

		Assert.assertTrue("Rename was not successful", file1.renameTo(file2));
		Assert.assertEquals("Renamed file path is wrong", localBaseDir + File.separator + "myfolder9/baker9.txt", file1.getPath());
		Assert.assertEquals("Renamed file canonical path is wrong", localBaseDir + File.separator + "myfolder9/baker9.txt", file1.getCanonicalPath());
		Assert.assertEquals("Renamed file name is wrong", "baker9.txt", file1.getName());
	}
}
