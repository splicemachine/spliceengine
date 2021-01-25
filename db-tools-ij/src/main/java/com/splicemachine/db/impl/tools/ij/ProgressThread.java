package com.splicemachine.db.impl.tools.ij;

import java.io.PrintStream;
import java.sql.SQLException;
import java.util.function.Supplier;

public class ProgressThread extends Thread {

    private int currentJob = -1;
    private int currentStage = -1;
    private int currentPercentage = 0;
    public int numSleepMs = 500;
    boolean startedProgress = false;

    private String firstRunning = "[Running ...                                                                                        ]";
    private String progressBar =  "[------------+----------25%----------+-----------50%-----------+----------75%-----------+-----------]";
    private byte[] progressBarB = progressBar.getBytes();
    private int LENGTH = progressBarB.length;

    Supplier<String> runningOp;
    PrintStream out;
    public ProgressThread(Supplier<String> op, PrintStream out) {
        runningOp = op;
        this.out = out;
    }

    public static class ProgressInfo {
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
            sb.append(jobname);
            sb.append("\n");
            sb.append(jobNumber); sb.append(" ");
            sb.append(numCompletedStages); sb.append(" ");
            sb.append(numStages); sb.append(" ");
            sb.append(numCompletedTasks); sb.append(" ");
            sb.append(numActiveTasks); sb.append(" ");
            sb.append(numTasks);
        }
        public String toString() {
            StringBuilder sb = new StringBuilder();
            toString(sb);
            return sb.toString();
        }

        ProgressInfo(String rc) {
            jobname = rc.split("\n")[0];
            String arr[] = rc.split("\n")[1].split(" ");
            jobNumber = Integer.parseInt(arr[0]);
            numCompletedStages = Integer.parseInt(arr[1]);
            numStages = Integer.parseInt(arr[2]);
            numCompletedTasks = Integer.parseInt(arr[3]);
            numActiveTasks = Integer.parseInt(arr[4]);
            numTasks = Integer.parseInt(arr[5]);
        }

        String jobname;
        int jobNumber;
        int numCompletedStages;
        int numStages;
        int numCompletedTasks;
        int numActiveTasks;
        int numTasks;
    }

    public ProgressThread(ij ijParser, String command, PrintStream out) {
        runningOp = () -> {
            try {
                return ijParser.getRunningOperation(command);
            } catch (SQLException e) {
                return "";
            }
        };
        this.out = out;
    }

    private void printProgress(int i) {
        out.print( (char)progressBarB[i] );
    }

    private void finish() {
        if(!startedProgress) return;
        for(int i=currentPercentage; i<LENGTH; i++) {
            printProgress(i);
        }
        out.print("\n");
    }

    private void updateProgress(ProgressInfo pi) {
        startedProgress = true;

        if( currentJob != pi.jobNumber || currentStage != pi.numCompletedStages) {
            if(currentJob != -1)
                finish();
            currentJob = pi.jobNumber;
            currentStage = pi.numCompletedStages;
            currentPercentage = 0;
            out.println(pi.jobname + " (Job " + (pi.jobNumber+1) + ", Stage " + (pi.numCompletedStages+1) + " of " + pi.numStages + ")");
        }
        int perc = (pi.numCompletedTasks*LENGTH)/pi.numTasks;
        for(int i=currentPercentage; i<perc; i++) {
            printProgress(i);
        }
        currentPercentage = perc;
    }

    public void updateProgress(String s) {
        if( s.length() > 0 )
            updateProgress( new ProgressInfo(s) );
    }

    public void run() {
        out.println(firstRunning);
        while (true) {
            String rc = runningOp.get();
            try {
                updateProgress(rc);
            }
            catch( java.lang.NumberFormatException | java.lang.ArrayIndexOutOfBoundsException e) {
            }
            try {
                Thread.sleep(numSleepMs);
            } catch (InterruptedException e) {
                finish();
                out.print("\n");
                return;
            }
        }
    }

    public void stopProgress() {
        interrupt();
        try {
            join();
        } catch (InterruptedException e) {
        }
    }
}
