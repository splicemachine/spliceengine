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

/**
 * This package represents uniform bit-positioned indices.
 *
 * The purpose of this package is to provide serializable bitmap indices which can compactly index small
 * portions of data which may be sparsely populated.
 *
 * All Indices constructed by implementations in this package correspond to indices which
 * can be serialized and deserialized into compact, bit-positioned indices. For example, an index
 * listing all the occurences of value {@code k} in a given multiset can be indexed here as offsets from
 * position zero.
 *
 * The primary (and intended) use-case for this collection are for byte-packing fixed-width rows into
 * a single byte array for efficient storage, compression, etc. in HBase, although they are useful for
 * other situations in which a bitmap index might be desired as well.
 *
 *
 * Different index implementations may be used in different contexts, but ALL indices must adhere to the
 * following format:
 *
 * The first 3 bits is a header bit, describing several aspects of the bitmap as stored:
 *
 * The first header bit indicates whether or not the implementation is densely formatted. In a densely-formatted
 * bitmap, all values are explicitly represented, even if there are zeros, while a Sparsely-formatted bitmap
 * would store only the entries which are present in the bitmap. As a full example, consider a bitmap with
 * 12 entries, of which entry 3, 5, 7 and 9 are set to 1, and all others are zero. In this case, a dense bitmap
 * is equivalent to 000101010100 (in binary), while a sparse bitmap would be 3579 (in some clever encoding scheme.
 * In this way, large, sparse bitmaps can be efficiently represented. The first header bit is set to {@code 1} if
 * the encoding is dense, {@code 0 } if it is sparse.
 *
 * The second header bit indicates whether or not the bitmap is compressed. In general, the compression
 * scheme is likely dependent on whether or not the bitmap is sparse or compressed. If the second header bit is set
 * to {@code 1}, then the bitmap is compressed. Otherwise, it is uncompressed.
 *
 * The third header bit indicates whether or not the data that is indexed is compressed (and hence requires
 * a decompression stage to perform an index lookup).
 *
 * The fourth header bit indicates a special case--when all entries of the data set itself are present (that is,
 * the bitmap consists of only 1s), then this bit is set to {@code 1}. This way, arbitrarily large indices
 * can be compactly represented when all entries are placed.
 *
 *
 */
package com.splicemachine.storage.index;
