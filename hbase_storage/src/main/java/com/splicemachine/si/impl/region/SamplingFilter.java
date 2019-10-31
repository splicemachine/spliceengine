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
 *
 */

package com.splicemachine.si.impl.region;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.util.random.BernoulliSampler;
import org.apache.spark.util.random.RandomSampler;
import scala.reflect.ClassTag;

import java.io.IOException;
import java.io.Serializable;

public class SamplingFilter extends FilterBase implements Serializable {

    private double rate;
    private RandomSampler sampler;

    public SamplingFilter(){
    }

    public SamplingFilter(double rate) {
        this.rate = rate;
        ClassTag<String> tag = scala.reflect.ClassTag$.MODULE$.apply(String.class);
        this.sampler = new BernoulliSampler(rate, tag);
    }

    @Override
    public boolean filterRowKey(byte[] buffer, int offset, int length) throws IOException {
        if (length == 0) return false; // make sure we don't remove special Memstore BEGIN / FLUSH rows
        return sampler.sample() <= 0;
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        return ReturnCode.INCLUDE;
    }

    @Override
    public byte[] toByteArray() throws IOException {
        return Bytes.toBytes(rate);
    }

    public static SamplingFilter parseFrom(final byte [] pbBytes)
            throws DeserializationException {
        double rate = Bytes.toDouble(pbBytes);
        return new SamplingFilter(rate);
    }
}
