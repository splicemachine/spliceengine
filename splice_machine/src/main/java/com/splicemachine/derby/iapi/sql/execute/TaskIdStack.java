package com.splicemachine.derby.iapi.sql.execute;

import com.google.common.collect.Lists;

import java.util.LinkedList;

/**
 * Holds thread-scoped stack of tasks Ids.
 */
public class TaskIdStack {

    /**
     * A chain of tasks for identifying parent and child tasks. The last byte[] in
     * the list is the immediate parent of other tasks.
     */
    private static final ThreadLocal<LinkedList<byte[]>> taskChain = new ThreadLocal<>();

    /**
     * Get the entire stack as a list, most recently added taskIds will be at the end of the list.
     */
    public static LinkedList<byte[]> getCurrentThreadTaskIdList() {
        return taskChain.get();
    }

    /**
     * Add taskId
     */
    public static void pushTaskId(byte[] taskId) {
        LinkedList<byte[]> list = taskChain.get();
        if (list == null) {
            //LL used to avoid wasting space here
            list = Lists.newLinkedList();
            taskChain.set(list);
        }
        list.addLast(taskId);
    }

    /**
     * Remove taskId
     */
    public static void popTaskId() {
        LinkedList<byte[]> bytes = taskChain.get();
        bytes.removeLast();
        if (bytes.isEmpty()) {
            taskChain.remove();
        }
    }

}