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

package com.splicemachine.pipeline.constraint;

import com.splicemachine.ddl.DDLMessage.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.ArrayUtils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

/**
 * Immutable class representing a named constraint on a named table.
 */
public class ConstraintContext implements Externalizable {

    /* The message args which will be passed to our StandardException factory method for creating a constraint
     * violation message appropriate for the constraint. */
    private String[] messageArgs;

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // convenient factory methods for various types of constraints
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public static ConstraintContext empty() {
        return new ConstraintContext(null);
    }

    public static ConstraintContext unique(String tableName, String constraintName){
        return new ConstraintContext(constraintName, tableName);
    }

    public static ConstraintContext primaryKey(String tableName, String constraintName){
        return new ConstraintContext(constraintName, tableName);
    }

    public static ConstraintContext foreignKey(FKConstraintInfo fkConstraintInfo) {
        String tableName = fkConstraintInfo.getTableName();
        String constraintName = fkConstraintInfo.getConstraintName();
        String columnNames = fkConstraintInfo.getColumnNames();
        return new ConstraintContext(constraintName, tableName, "Operation", "(" + columnNames + ")");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /* For serialization */
    @Deprecated
    public ConstraintContext() {
    }

    /* Use factory methods above instead for clarity */
    public ConstraintContext(String... messageArgs) {
        this.messageArgs = messageArgs;
    }

    /* Copy but with specified argument inserted at specified index */
    public ConstraintContext withInsertedMessage(int index, String newMessage) {
        return new ConstraintContext((String[]) ArrayUtils.add(messageArgs,index,newMessage));
    }

    /* Copy but with specified argument removed */
    public ConstraintContext withoutMessage(int index) {
        return new ConstraintContext((String[]) ArrayUtils.remove(messageArgs,index));
    }

    /* Copy but with specified argument set at specified index */
    public ConstraintContext withMessage(int index, String newMessage) {
        String[] newArgs = Arrays.copyOf(this.messageArgs, this.messageArgs.length);
        newArgs[index] = newMessage;
        return new ConstraintContext(newArgs);
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public String[] getMessages() {
        return messageArgs;
    }

    @Override
    public boolean equals(Object o) {
        return (this == o) || (o instanceof ConstraintContext) &&
                Arrays.equals(this.messageArgs, ((ConstraintContext) o).messageArgs);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(messageArgs);
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        short len = objectInput.readShort();
        if (len > 0) {
            messageArgs = new String[len];
            for (int i = 0; i < messageArgs.length; i++) {
                messageArgs[i] = objectInput.readUTF();
            }
        }
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        short len = (short) (messageArgs == null ? 0 : messageArgs.length);
        objectOutput.writeShort(len);
        for (int i = 0; i < len; i++) {
            objectOutput.writeUTF(messageArgs[i]);
        }
    }

    @Override
    public String toString() {
        return "ConstraintContext{" +
                "messageArgs=" + Arrays.toString(messageArgs) +
                '}';
    }
}
