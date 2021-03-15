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

package com.splicemachine.derby.stream.function.csv;

import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class CsvParserConfig {
    CsvPreference preferences;
    boolean oneLineRecord;
    boolean quotedEmptyIsNull;
    boolean preserveLineEndings;

    public CsvParserConfig(CsvPreference preferences) {
        this.preferences = preferences;
        this.oneLineRecord = false;
        this.quotedEmptyIsNull = false;
        this.preserveLineEndings = true;
    }
    public CsvParserConfig(CsvPreference preferences,
                           boolean oneLineRecord, boolean quotedEmptyIsNull, boolean preserveLineEndings) {
        this.preferences = preferences;
        this.oneLineRecord = oneLineRecord;
        this.quotedEmptyIsNull = quotedEmptyIsNull;
        this.preserveLineEndings = preserveLineEndings;
    }
    public CsvParserConfig oneLineRecord(boolean value) {
        oneLineRecord = value;
        return this;
    }

    public CsvParserConfig quotedEmptyIsNull(boolean value) {
        quotedEmptyIsNull = value;
        return this;
    }
    public CsvParserConfig preserveLineEndings(boolean value) {
        preserveLineEndings = value;
        return this;
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(oneLineRecord);
        out.writeBoolean(quotedEmptyIsNull);
        out.writeBoolean(preserveLineEndings);
    }

    CsvParserConfig(ObjectInput in) throws IOException {
        oneLineRecord  = in.readBoolean();
        quotedEmptyIsNull = in.readBoolean();
        preserveLineEndings = in.readBoolean();
        preferences = null;
    }
}
