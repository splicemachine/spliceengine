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
package com.splicemachine.orc.stream;

import javax.annotation.Nullable;
import java.io.IOException;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class ValueStreamSource<S extends ValueStream<?>> implements StreamSource<S>
{
    private final S stream;

    public ValueStreamSource(S stream)
    {
        this.stream = requireNonNull(stream, "stream is null");
    }

    @Override
    public Class<S> getStreamType()
    {
        return (Class<S>) stream.getClass();
    }

    @Nullable
    @Override
    public S openStream()
            throws IOException
    {
        return stream;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("stream", stream)
                .toString();
    }
}
