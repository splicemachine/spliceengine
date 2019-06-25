/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.utils;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;

public class IndentedString implements Externalizable {
    //private static final long serialVersionUID = 4L;
    private static final int classVersion = 1;
    int          indentationLevel;
    List<String> textLines;

    public IndentedString() {

    }

    public IndentedString(int indentationLevel, String textLine) {
        this.indentationLevel = indentationLevel;
        this.textLines = new LinkedList<>();
        this.textLines.add(textLine);
    }

    public IndentedString(int indentationLevel, List<String> textLines) {
        this.indentationLevel = indentationLevel;
        this.textLines = textLines;
    }

    public int getIndentationLevel() { return indentationLevel; }
    public void setIndentationLevel(int newLevel) { indentationLevel = newLevel; }

    public List<String> getTextLines() { return textLines; }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(classVersion);
        out.writeInt(indentationLevel);
        out.writeInt(this.textLines.size());
        for (String s:this.textLines)
            out.writeUTF(s);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        int classVersion = in.readInt();
        indentationLevel = in.readInt();
        int numItems = in.readInt();
        textLines = new LinkedList<>();
        for (int i = 0; i < numItems; i++) {
            textLines.add(in.readUTF());
        }
    }
}