package com.splicemachine.utils;

import com.splicemachine.constants.bytes.BytesUtil;

import java.util.UUID;

/**
 * Uses Java standard library classes to generate 128-bit UUIDs
 * @author Scott Fines
 * Date: 2/26/14
 */
public class BasicUUIDGenerator implements UUIDGenerator{
		@Override
		public byte[] nextBytes() {
				UUID next = UUID.randomUUID();
				byte[] data = new byte[16];
				BytesUtil.longToBytes(next.getMostSignificantBits(),data,0);
				BytesUtil.longToBytes(next.getLeastSignificantBits(),data,8);
				return data;
		}

		@Override
		public int encodedLength() {
				return 16;
		}

		@Override
		public void next(byte[] data, int offset) {
				UUID next = UUID.randomUUID();
				BytesUtil.longToBytes(next.getMostSignificantBits(),data,offset);
				BytesUtil.longToBytes(next.getLeastSignificantBits(),data,offset+8);
		}
}
