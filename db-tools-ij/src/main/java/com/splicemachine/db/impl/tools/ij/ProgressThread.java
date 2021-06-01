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
package com.splicemachine.db.impl.tools.ij;

import com.splicemachine.db.shared.ProgressInfo;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.PrintStream;
import java.sql.SQLException;
import java.util.function.Supplier;

/**
 * Thread to display progress of the current running operation on the sqlshell
 */
@SuppressFBWarnings("DM_DEFAULT_ENCODING") // progressBar is ASCII always
public class ProgressThread extends Thread {

    private int currentJob = -1;
    private int currentStage = -1;
    private int currentPercentage = 0;
    public int timeBetweenUpdates = 500;
    boolean startedProgress = false;

    private String firstRunning = "[Running ...                                                                                        ]";
    private String progressBar =  "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]";
    private byte[] progressBarB = progressBar.getBytes();
    private int PROGRESS_BAR_LENGTH = progressBarB.length;
    private String lastJobName = "";

    Supplier<String> progressInfoProvider;
    PrintStream outputStream;
    boolean started = false;
    boolean hadException = false;
    public boolean canceled = false;
    public boolean smallJobs = false;
    int smallJobCompleted =0 ;

    boolean firstRunningPrinted = false;

    // used in tests only
    public ProgressThread(Supplier<String> op, PrintStream outputStream) {
        progressInfoProvider = op;
        this.outputStream = outputStream;
    }

    public ProgressThread(ij ijParser, String command, PrintStream outputStream) {
        progressInfoProvider = () -> {
            try {
                return ijParser.getProgress(command);
            } catch (SQLException e) {
                return "";
            }
        };
        this.outputStream = outputStream;
    }

    @Override
    public void start() {
        started = true;
        super.start();
    }

    private void printProgressBarChar(int i) {
        if(i < PROGRESS_BAR_LENGTH)
            outputStream.print( (char)progressBarB[i] );
    }

    public void finishCurrentProgressBar() {
        if(!startedProgress) return;
        for(int i = currentPercentage; i< PROGRESS_BAR_LENGTH; i++) {
            printProgressBarChar(i);
        }
        outputStream.print("\n");
    }

    public void updateProgress(ProgressInfo pi) {
        if(pi.isInvalid()) return;
        printFirstRunning();
        startedProgress = true;
        int numCompleted = pi.getNumCompletedTasks();
        int numTasks = pi.getNumTasks();

        if( currentJob != pi.getJobNumber() || currentStage != pi.getNumCompletedStages()) {
            currentStage = pi.getNumCompletedStages();
            if(smallJobs && pi.getNumTasks() <= 2 && smallJobCompleted < PROGRESS_BAR_LENGTH) {
                numCompleted = ++smallJobCompleted;
                numTasks = PROGRESS_BAR_LENGTH;
            }
            else {
                // JOB or STAGE has changed
                if (currentJob != -1)
                    finishCurrentProgressBar();
                currentJob = pi.getJobNumber();
                currentPercentage = 0;
                // jobname might include SQL statement which can be long and multi-lined,
                // so don't repeat the job name if it doesn't change
                String jobName = (lastJobName != null && lastJobName.equals(pi.getJobname())) ? "" : pi.getJobname() + " ";
                lastJobName = pi.getJobname();
                if (pi.getNumTasks() <= 2) {
                    outputStream.println(jobName + "(Collection of Jobs)");
                    smallJobs = true;
                    smallJobCompleted = 0;
                    numCompleted = 0;
                    numTasks = PROGRESS_BAR_LENGTH;
                } else {
                    smallJobs = false;
                    outputStream.println(jobName + "(Job " + (pi.getJobNumber() + 1) + ", Stage "
                            + (pi.getNumCompletedStages() + 1) + " of " + pi.getNumStages() + ")");
                }
            }
        }

        int perc = (numCompleted* PROGRESS_BAR_LENGTH)/numTasks;
        if(perc < currentPercentage)
            return; // ignore progress going backward
        for(int i=currentPercentage; i<perc; i++) {
            printProgressBarChar(i);

        }
        currentPercentage = perc;
    }

    public void updateProgress(String s) {
        if( s.length() > 0 && !s.equals("-") ) {
            try {
                //outputStream.println(s);
                updateProgress(ProgressInfo.deserializeFromString(s));
            } catch (Exception e) {
                if(!hadException) {
                    outputStream.println("\nException while parsing/updating ProgressInfo: " + e.toString());
                    hadException = true; // ignore following to not spam
                }
            }
        }
    }

    void printFirstRunning() {
        if(firstRunningPrinted) return;
        outputStream.println(firstRunning);
        firstRunningPrinted = true;
    }

    public void run() {
        while(true) {
            String str = progressInfoProvider.get();
            updateProgress(str);
            try {
                Thread.sleep(timeBetweenUpdates);
            } catch (InterruptedException e) {
                if(!canceled && firstRunningPrinted) {
                    finishCurrentProgressBar();
                    outputStream.print("\n");
                }
                return;
            }
            printFirstRunning();
        }
    }

    public void stopProgress() {
        if( !started ) return;
        interrupt();
        try {
            join();
        } catch (InterruptedException e) {
        }
        started = false;
    }
}
