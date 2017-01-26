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

package com.splicemachine.derby.impl.sql.actions.index;

import org.spark_project.guava.io.Files;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * CSV test file manipulation
 */
public class CsvUtil {

    public static Collection<String> insertString(String dirName, String fileName, int colNum, String chars) {
        List<String> lines = fileToLines(dirName + fileName, "--");
        List<String> outLines =new ArrayList<>(lines.size());

        StringBuilder aLine = new StringBuilder();
        for (String line : lines) {
            String[] cols = line.split(",");
            if (cols.length <= colNum-1) {
                throw new RuntimeException("Index "+(colNum-1)+" is greater than size - "+cols.length);
            }
            aLine.setLength(0);
            for (int i=0; i<cols.length; i++) {
                if (i == colNum-1) {
                    aLine.append(chars).append(",");
                } else {
                    aLine.append(cols[i]).append(",");
                }
            }
            outLines.add(aLine.toString());
        }

        return outLines;
    }

    public static Collection<String> makeUnique(String dirName, String fileName, int[] colNums) {
        List<String> lines = fileToLines(dirName + fileName, "--");
        Map<String, String> unique =new HashMap<>(lines.size());

        StringBuilder key = new StringBuilder();
        for (String line : lines) {
            String[] cols = line.split(",");
            if (cols.length <= max(colNums)) {
                throw new RuntimeException("Index "+max(colNums)+" is greater than size - "+cols.length);
            }
            key.setLength(0);
            for (int colNum : colNums) {
                key.append(cols[colNum]).append("|");
            }
            if (unique.containsKey(key.toString())) {
                System.out.println("Collision on key: "+key.toString());
            }
            unique.put(key.toString(), line);
        }
        return unique.values();
    }

    public static Collection<String> getLinesWithValueInColumn(String dirName,
                                                                String fileName,
                                                                int col,
                                                                String colVal) {
        List<String> linesWithValue =new ArrayList<>();
        for (String line : fileToLines(dirName + fileName, "--")) {
            String[] cols = line.split(",");
            if (cols.length > col && cols[col].equalsIgnoreCase(colVal)) {
                linesWithValue.add(line);
            }
        }

        return linesWithValue;
    }

    public static int findColumn(String colName, String schemaDef) {
        int i=0;
        for (String col : schemaDef.split(",")) {
            String[] parts = col.split(" ");
            if (parts.length > 0 && ! parts[0].isEmpty() && parts[0].equalsIgnoreCase(colName)) {
                return i;
            }
            ++i;
        }
        return -1;
    }

    public static void writeLines(String dirName, String fileName, Collection<String> content) throws Exception {
        File targetFile = new File(dirName,fileName);
        Files.createParentDirs(targetFile);
        if (targetFile.exists())
            targetFile.delete();
        targetFile.createNewFile();
        FileUtils.writeLines(targetFile, content);
    }

    private static int max(int[] nums) {
        int max = 0;
        for (int num : nums) {
            if (num >= max) {
                max = num;
            }
        }
        return max;
    }

    // TODO - this client blows memory, of course, when there's too many lines in the file
    public static List<String> fileToLines(String filePath, String commentPattern) {
        List<String> lines = new LinkedList<String>();
        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(filePath));

            String line = in.readLine();
            while(line != null) {
                if (commentPattern != null) {
                    if (! lineIsComment(line, commentPattern)) {
                        lines.add(line);
                    }
                } else {
                    lines.add(line);
                }
                line = in.readLine();
            }
        } catch (IOException e) {
            Assert.fail("Unable to read: " + filePath + ": " + e.getLocalizedMessage());
        } finally {
        	if (in != null) {
        		try {
					in.close();
				} catch (IOException e) {
					// ignore
				}
        	}
        }
        return lines;
    }

    private static boolean lineIsComment(String line, String commentPattern) {
        return !(commentPattern == null || commentPattern.isEmpty()) && line.trim().startsWith(commentPattern);
    }

    public static String getResourceDirectory() {
        String userDir = System.getProperty("user.dir");
        if(!userDir.endsWith("splice_machine"))
            userDir = userDir+"/splice_machine";
        return userDir+"/src/test/test-data";
    }

}
