/*
 * Copyright (c) 2020 - 2021 Splice Machine, Inc.
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

package com.splicemachine.db.shared;

/**
 *  Information about the progress of a running operation.
 *  JOB: exactly ONE job running at each time
 *  JOB is sequence of STAGES:
 *       STAGE exactly ONE stage running at each time
 *       STAGE is a sequence of TASKS:
 *           TASK is a work unit that can all be run together, in parallel, without a shuffle
 *  Since we're mostly executing Spark Tasks, this definition is taken from Spark:
 *  see https://queirozf.com/entries/apache-spark-architecture-overview-clusters-jobs-stages-tasks
 */
public class ProgressInfo {
    private String jobname;
    private int jobNumber;

    private int numCompletedStages;
    private int numStages;

    private int numCompletedTasks;
    private int numActiveTasks;
    private int numTasks;

    public String getJobname() {
        return jobname;
    };
    public int getJobNumber() {
        return jobNumber;
    };

    public int getNumCompletedStages() {
        return numCompletedStages;
    };
    public int getNumStages() {
        return numStages;
    };

    public int getNumCompletedTasks() {
        return numCompletedTasks;
    };
    public int getNumActiveTasks() {
        return numActiveTasks;
    };
    public int getNumTasks() {
        return numTasks;
    };
    public String VERSION = "1";

    public ProgressInfo(String jobname,
                        int jobNumber,
                        int numCompletedStages,
                        int numStages,
                        int numCompletedTasks,
                        int numActiveTasks,
                        int numTasks) {
        this.jobname = jobname;
        this.jobNumber = jobNumber;
        this.numCompletedStages = numCompletedStages;
        this.numStages = numStages;
        this.numCompletedTasks = numCompletedTasks;
        this.numActiveTasks = numActiveTasks;
        this.numTasks = numTasks;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + jobname.hashCode();
        hash = 31 * hash + Integer.hashCode(jobNumber);
        hash = 31 * hash + Integer.hashCode(numCompletedStages);
        hash = 31 * hash + Integer.hashCode(numStages);
        hash = 31 * hash + Integer.hashCode(numCompletedTasks);
        hash = 31 * hash + Integer.hashCode(numActiveTasks);
        hash = 31 * hash + Integer.hashCode(numTasks);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;
        ProgressInfo pi = (ProgressInfo) obj;
        return pi.jobname.equals(this.jobname) &&
                pi.numCompletedStages == this.numCompletedStages &&
                pi.numStages == this.numStages &&
                pi.numCompletedTasks == this.numCompletedTasks &&
                pi.numActiveTasks == this.numActiveTasks &&
                pi.numTasks == this.numTasks;
    }

    // adopted from https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/ui/ConsoleProgressBar.scala
    String getSparkProgressIndicator(StringBuilder sb, int width) {
        String header = "[ Job " + jobNumber + ", Stage " + (numCompletedStages + 1) + " / " + numStages + " ";
        String tailer = " (" + numCompletedTasks + " + " + numActiveTasks + ") / " + numTasks + " ]";
        sb.append(header);
        int w = width - header.length() - tailer.length();
        if (w > 0) {
            int percent = w * numCompletedTasks / numTasks;
            for (int i = 0; i < w; i++) {
                if (i < percent)
                    sb.append('=');
                else if (i == percent)
                    sb.append('>');
                else
                    sb.append('-');
            }
        }
        sb.append(tailer);
        return sb.toString();
    }

    public void toString(StringBuilder sb) {

    }

    public String toString() {
        return serializeToString();
    }

    public static ProgressInfo deserializeFromString(String str) {
        return new ProgressInfo().deserializeFromStringInternal(str);
    }

    private ProgressInfo() { };

    // outputting in format
    // VERSION;jobNumber numCompletedStages numStages numCompletedTasks numActiveTasks
    // jobname (may include \n)

    public String serializeToString() {
        StringBuilder sb = new StringBuilder();
        sb.append(VERSION); sb.append(";");
        sb.append(jobNumber); sb.append(" ");
        sb.append(numCompletedStages); sb.append(" ");
        sb.append(numStages); sb.append(" ");
        sb.append(numCompletedTasks); sb.append(" ");
        sb.append(numActiveTasks); sb.append(" ");
        sb.append(numTasks);
        sb.append("\n");
        sb.append(jobname);
        return sb.toString();
    }

    ProgressInfo deserializeFromStringInternal(String str) {
        if(!str.startsWith(VERSION + ";"))
            throw new RuntimeException("progress information version has changed, expected " + VERSION);
        str = str.substring(VERSION.length()+1);
        int sep = str.indexOf('\n');
        jobname = str.substring(sep+1);
        jobname = jobname.replace("<br/>", "\n");
        String arr[] = str.substring(0, sep).split(" ");
        jobNumber = Integer.parseInt(arr[0]);
        numCompletedStages = Integer.parseInt(arr[1]);
        numStages = Integer.parseInt(arr[2]);
        numCompletedTasks = Integer.parseInt(arr[3]);
        numActiveTasks = Integer.parseInt(arr[4]);
        numTasks = Integer.parseInt(arr[5]);
        return this;
    }

    public boolean isInvalid() {
        return jobNumber < 0 || numCompletedStages < 0 || numStages < 0 || numCompletedStages < 0
                || numActiveTasks < 0 || numTasks < 0 || numCompletedTasks > numTasks || numActiveTasks > numTasks
                || numCompletedStages > numStages;
    }
}
