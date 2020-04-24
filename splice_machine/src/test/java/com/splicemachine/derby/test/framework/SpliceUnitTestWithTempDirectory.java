package com.splicemachine.derby.test.framework;

import org.apache.commons.io.FileUtils;
import org.junit.After;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/// this class extends SpliceUnitTest so that each test
/// can access a temporary output directory that is removed after the test is run
/// the temporary output directory is created only on demand.
/// might be also doable like SpliceSchemaWatcher extends TestWatcher
public class SpliceUnitTestWithTempDirectory extends SpliceUnitTest
{
    /// set the following boolean to true to prevent deletion of temporary files (e.g. for debugging)
    public boolean debug_no_delete = false;
    /// the temporary directory (created on demand)
    private Path tempDir = null;

    /// this will be called after each test, deleting possible temp directories.
    @After
    public void deleteTempDirectory() throws Exception
    {
        if( tempDir == null )
            return;
        else if( debug_no_delete )
            System.out.println( "Not deleting test temporary directory " + tempDir );
        else
            FileUtils.deleteDirectory( tempDir.toFile() );
    }

    /// this will return the temp directory, that is created on demand once for each test
    public String getTempOutputDirectory() throws Exception
    {
        if( tempDir == null)
            tempDir = Files.createTempDirectory( "SpliceUnitTest_temp_" );
        // add '/' to avoid leaking directories if someone misses the '/'
        return tempDir.toString() + "/";
    }

    /// creates a temporary file in the temporary directory, so will be deleted after test
    public File createTempOutputFile( String prefix, String suffix ) throws Exception
    {
        return File.createTempFile(prefix, suffix, new File(getTempOutputDirectory()) );
    }

    /// copy subfolder of resource directory (i.e. GITROOT/splice_machine/src/test/test-data/)
    /// to temporary directory @sa getTempOutputDirectory
    /// @return destination directory as string
    public String getTempCopyOfResourceDirectory( String subfolder ) throws Exception
    {
        File src = new File(SpliceUnitTest.getResourceDirectory() + subfolder);
        File dest = new File( getTempOutputDirectory() + subfolder );
        FileUtils.copyDirectory( src, dest );
        return dest.toString();
    }

    /// @return number of files in directory tree
    // recursively, going down directories, but not counting directories as files
    public static int getNumberOfFiles(String dirPath)
    {
        int count = 0;
        File f = new File(dirPath);
        File[] files = f.listFiles();

        if (files == null)
            return 0;

        for (File file : files)
        {
            count++;
            if (file.isDirectory()) {
                count += getNumberOfFiles(file.getAbsolutePath());
            }
        }
        return count;
    }
}
