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

package com.splicemachine.derby.impl.load;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.FileInfo;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 2/26/14
 */
public class ImportUtils{

    /**
     * Return the total space consumed by the import data files.
     *
     * @return total space consumed by the import data files
     * @throws IOException
     */
    public static FileInfo getImportFileInfo(String filePath) throws StandardException, IOException{
        DistributedFileSystem fsLayer=getFileSystem(filePath);
        return fsLayer.getInfo(filePath);
    }

    /**
     * Check that the file given by the string path (or directory, if flagged) exists and is readable.
     * @param file the absolute path to the file
     * @param checkDirectory check as a directory if this flag set
     * @throws StandardException
     */
    public static void validateReadable(String file,boolean checkDirectory) throws StandardException{
        if(file==null) return;

        DistributedFileSystem fsLayer=getFileSystem(file);
        FileInfo info;
        try{
            info=fsLayer.getInfo(file);
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
        if(! info.exists() || (checkDirectory && info.isDirectory()))
            throw ErrorState.LANG_FILE_DOES_NOT_EXIST.newException(file);
        if(! info.isReadable()){
            throw ErrorState.LANG_NO_READ_PERMISSION.newException(info.getUser(),info.getGroup(),file);
        }
    }

    /**
     * ValidateWritable FileSystem
     *
     * @param path the path to check
     * @param checkDirectory if true, then ensure that the file specified by {@code path} is not
     *                       a directory
     * @throws StandardException
     */
    public static void validateWritable(String path,boolean checkDirectory) throws StandardException{
        //check that the badLogDirectory exists and is writable
        if(path==null) return;
        DistributedFileSystem fsLayer = getFileSystem(path);
        FileInfo info;

        try{
            info = fsLayer.getInfo(path);
        }catch(Exception e){
            throw StandardException.plainWrapException(e);
        }

        URI uri = URI.create(path);
        String scheme = uri.getScheme();
        if (scheme == null || scheme.compareToIgnoreCase("S3A") != 0) {
            // PrestoS3AFileSystem has problem reading an empty folder. It cannot determine whether the folder does not
            // exist, or the folder is empty. Skip checking for S3A. If the directory does not exist, it will be
            // created.
            if (checkDirectory && !info.isDirectory()) {
                throw ErrorState.DATA_FILE_NOT_FOUND.newException(path);
            }
            if (!info.isWritable()) {
                throw ErrorState.LANG_NO_WRITE_PERMISSION.newException(info.getUser(), info.getGroup(), path);
            }
        }
    }


    public static DistributedFileSystem getFileSystem(String path) throws StandardException {
        try {
           return SIDriver.driver().getSIEnvironment().fileSystem(path);
        } catch (URISyntaxException e) {
            throw StandardException.newException(SQLState.FILESYSTEM_URI_EXCEPTION,e.getMessage());
        } catch (Exception ioe) { // Runtime is added to handle Amazon S3 Issues
            throw StandardException.newException(SQLState.FILESYSTEM_IO_EXCEPTION, ioe.getMessage());
        }
    }

    public static String unescape(String str) throws IOException {
        if (str == null || str.toUpperCase().equals("NULL")) {
            return str;
        }
        StringBuilder unescaped = new StringBuilder(4);
        int sz = str.length();
        boolean hadControl = false;
        boolean hadBackslash = false;
        for (int i = 0; i < sz; i++) {
            char ch = str.charAt(i);
            if (hadControl) {
                // support ctrl chars
                switch (ch) {
                    case 'A':
                    case 'a':
                        unescaped.append('\u0001');
                        break;
                    case 'M':
                    case 'm':
                        unescaped.append('\n');
                        break;
                    default:
                        throw new IOException("Unsupported control char '"+str+"'");
                }
                continue;
            } else if (hadBackslash) {
                // handle an escaped value
                hadBackslash = false;
                switch (ch) {
                    case '\\':
                        unescaped.append('\\');
                        break;
                    case '\'':
                        unescaped.append('\'');
                        break;
                    case '\"':
                        unescaped.append('"');
                        break;
                    case 'r':
                        unescaped.append('\r');
                        break;
                    case 'f':
                        unescaped.append('\f');
                        break;
                    case 't':
                        unescaped.append('\t');
                        break;
                    case 'n':
                        unescaped.append('\n');
                        break;
                    case 'b':
                        unescaped.append('\b');
                        break;
                    default :
                        throw new IOException("Unsupported escape char '"+str+"'");
                }
                continue;
            } else if (ch == '\\') {
                hadBackslash = true;
                continue;
            } else if (ch == '^') {
                hadControl = true;
                continue;
            }
            unescaped.append(ch);
        }

        if (hadControl && unescaped.length() == 0)
            return "^";
        else
            return unescaped.toString();
    }

    /**
     * Output cut points to files
     * @param cutPointsList
     * @throws IOException
     */
    public static void dumpCutPoints(List<Tuple2<Long, byte[][]>> cutPointsList, String bulkImportDirectory) throws StandardException {

        BufferedWriter br = null;
        try {
            Configuration conf = (Configuration)SIDriver.driver().getConfiguration().getConfigSource().unwrapDelegate();
            FileSystem fs = FileSystem.get(URI.create(bulkImportDirectory), conf);

            for (Tuple2<Long, byte[][]> t : cutPointsList) {
                Long conglomId = t._1;

                Path path = new Path(bulkImportDirectory, conglomId.toString());
                FSDataOutputStream os = fs.create(new Path(path, "cutpoints"));
                br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );

                byte[][] cutPoints = t._2;

                for (byte[] cutPoint : cutPoints) {
                    br.write(Bytes.toStringBinary(cutPoint) + "\n");
                }
                br.close();
            }
        }catch (IOException e) {
            throw StandardException.plainWrapException(e);
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (IOException e) {
                throw StandardException.plainWrapException(e);
            }
        }
    }
}
