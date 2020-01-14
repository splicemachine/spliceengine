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
package com.splicemachine.orc.metadata;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ColumnEncoding
{
    public enum ColumnEncodingKind
    {
        DIRECT,
        DICTIONARY,
        DIRECT_V2,
        DICTIONARY_V2,
        DWRF_DIRECT,
    }

    private final ColumnEncodingKind columnEncodingKind;
    private final int dictionarySize;

    public ColumnEncoding(ColumnEncodingKind columnEncodingKind, int dictionarySize)
    {
        this.columnEncodingKind = requireNonNull(columnEncodingKind, "columnEncodingKind is null");
        this.dictionarySize = dictionarySize;
    }

    public ColumnEncodingKind getColumnEncodingKind()
    {
        return columnEncodingKind;
    }

    public int getDictionarySize()
    {
        return dictionarySize;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnEncodingKind", columnEncodingKind)
                .add("dictionarySize", dictionarySize)
                .toString();
    }
}
