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


package com.splicemachine.si.impl;

import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.sql.Row;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 * Class to serialize a query across the wire.
 *
 */
public class SpliceQuery implements Externalizable {
    public static final ThreadLocal<SpliceQuery> queryContext = new ThreadLocal<SpliceQuery>();
    ExecRow template;
    FormatableBitSet scanColumns;
    Qualifier[][] qualifiers;

    public SpliceQuery() {}

    /* msirek-temp:  should this file be removed?
    public SpliceQuery(ExecRow template) {
        FormatableBitSet scanColumns;
        scanColumns = new FormatableBitSet(template.nColumns());
        scanColumns.setAll();
        this.template = template;
        this.scanColumns = scanColumns;
    }

    public SpliceQuery(ExecRow template, FormatableBitSet scanColumns) {
        this(template,scanColumns,null);
    }

    public SpliceQuery(ExecRow template, FormatableBitSet scanColumns, Qualifier[][] qualifiers) {
        assert template!=null:"Passed in template is null";
        assert scanColumns!=null:"Passed in scanColumns is null";
        assert scanColumns!=null && (scanColumns.getNumBitsSet() == template.nColumns()):"Template, scancolumn Mix Match " + scanColumns + " : " + template;
        this.template=template;
        this.scanColumns = scanColumns;
        this.qualifiers = qualifiers;
    }
*/

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(scanColumns!=null);
        if (scanColumns != null)
            out.writeObject(scanColumns);
        out.writeObject(template);
        out.writeBoolean(qualifiers!=null);
        if (qualifiers != null) {
            out.writeInt(qualifiers.length);
            out.writeInt(qualifiers[0].length);
            for (int i = 0; i < qualifiers[0].length; i++) {
                out.writeObject(qualifiers[0][i]);
            }
            for (int and_idx = 1; and_idx < qualifiers.length; and_idx++) {
                out.writeInt(qualifiers[and_idx].length);
                for (int or_idx = 0; or_idx < qualifiers[and_idx].length; or_idx++) {
                    out.writeObject(qualifiers[and_idx][or_idx]);
                }
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        if (in.readBoolean())
            scanColumns = (FormatableBitSet) in.readObject();
        template = (ExecRow) in.readObject();
        if (in.readBoolean()) {
            qualifiers = new Qualifier[in.readInt()][];
            qualifiers[0] = new Qualifier[in.readInt()];
            for (int i = 0; i < qualifiers[0].length; i++) {
                qualifiers[0][i] = (Qualifier) in.readObject();
            }
            for (int and_idx = 1; and_idx < qualifiers.length; and_idx++) {
                qualifiers[and_idx] = new Qualifier[in.readInt()];
                for (int or_idx = 0; or_idx < qualifiers[and_idx].length; or_idx++) {
                    qualifiers[and_idx][or_idx] = (Qualifier) in.readObject();
                }
            }
        }
    }

    public ExecRow getTemplate() {
        return this.template;
    }

    public void setTemplate(ExecRow template) {
        this.template = template;
    }

    public FormatableBitSet getScanColumns() {
        return this.scanColumns;
    }

    public void setScanColumns(FormatableBitSet scanColumns) {
        this.scanColumns = scanColumns;
    }

    public Qualifier[][] getQualifiers() {
        return this.qualifiers;
    }

    public void setQualifiers(Qualifier[][] qualifiers) {
        this.qualifiers = qualifiers;
    }
}

