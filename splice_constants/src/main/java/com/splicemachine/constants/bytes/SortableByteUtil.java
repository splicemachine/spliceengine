package com.splicemachine.constants.bytes;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.Normalizer;
import java.util.Locale;
import org.apache.hadoop.hbase.util.Bytes;

public class SortableByteUtil {
	private static final int EXP_OFFSET = (int)Math.pow(2, 14); // half of the largest number that can be stored in 15 bits

	public static byte[] toBytes(Long value) {
		byte[] bytes = new byte[Bytes.SIZEOF_LONG];
		Bytes.putLong(bytes, 0, value);
		bytes[0] = (byte)(bytes[0] ^ 0x80);
		return bytes;
	}

	public static byte[] toBytes(Integer value) {
		byte[] bytes = new byte[Bytes.SIZEOF_INT];
		Bytes.putInt(bytes, 0, value);
		bytes[0] ^= 0x80;
		return bytes;
	}

	public static byte[] toBytes(Float value) {
		byte[] bytes = new byte[Bytes.SIZEOF_FLOAT];
		Bytes.putFloat(bytes, 0, value);
		int test = (bytes[0] >>> 7) & 0x01;
		if (test == 1) {
			// Negative numbers: invert all bits: sign, exponent and mantissa
			for (int i = 0; i < Bytes.SIZEOF_FLOAT; i++) {
				bytes[i] = (byte)(bytes[i] ^ 0xFF);
			}
		} else {
			// Positive numbers: invert the sign bit
			bytes[0] = (byte)(bytes[0] | 0x80);
		}
		return bytes;
	}

	public static byte[] toBytes(Double value) {        
		byte[] bytes = new byte[Bytes.SIZEOF_DOUBLE];
		Bytes.putDouble(bytes, 0, value);
		int test = (bytes[0] >>> 7) & 0x01;
		if (test == 1) {
			// Negative numbers: invert all bits: sign, exponent and mantissa
			for (int i = 0; i < Bytes.SIZEOF_DOUBLE; i++) {
				bytes[i] = (byte)(bytes[i] ^ 0xFF);
			}
		} else {
			// Positive numbers: invert the sign bit
			bytes[0] = (byte)(bytes[0] | 0x80);
		}
		return bytes;
	}

	public static byte[] toBytes(int length, byte[] value) {
		if (value.length == length)
			return value;
		byte[] bytes = new byte[length];
		int copyLength = value.length < length ? value.length : length;
		System.arraycopy(value, 0, bytes, 0, copyLength);
		return bytes;
	}

	public static byte[] toBytes(int length, BigDecimal value) {
		byte[] bytes = new byte[length];
		BigDecimal dec = (BigDecimal)value;
		BigInteger mantissa = dec.unscaledValue();
		int exp = dec.precision() - dec.scale();
		int signum = dec.signum();
		byte[] mantissaBytes = mantissa.toByteArray();
		// Mantissabytes are in two's complement, just invert the sign bit
		mantissaBytes[0] = (byte)(mantissaBytes[0] ^ 0x80);

		if ((exp > (EXP_OFFSET - 1)) || (exp < (-1 * EXP_OFFSET))) {
			throw new RuntimeException("Cannot convert number to sortable decimal: exp = " + exp + ", mantissa = [0.]" + mantissa);
		}

		// Similar to IEEE floating points, add an offset to the exponent so that it becomes 0-based.
		exp += EXP_OFFSET;

		// For negative numbers, flip the exponent
		if (signum < 0) {
			exp ^= 0xFF;
		}
		// Copy exponent into result array
		bytes[0] = (byte)exp;
		exp >>>= 8;
		bytes[1] = (byte)exp;

		// Set the sign bit
		if (signum >= 0) {
			bytes[0] = (byte)(bytes[0] | 0x80);
		} else {
			bytes[0] = (byte)(bytes[0] & 0x7F);
		}
		// Copy mantissa into result array
		int mantissaLength = Math.min(mantissaBytes.length, length - 2);
		System.arraycopy(mantissaBytes, 0, bytes, 2, mantissaLength);
		return bytes;
	}

	public static byte[] toBytes(Locale locale, boolean caseSensitive, String value) {
		value = Normalizer.normalize(value, Normalizer.Form.NFC);
		if (!caseSensitive)
			value = value.toLowerCase(locale);
		byte[] bytes = null;
		try {
			bytes = value.getBytes("UTF-8");
			for (int i = 0; i <= bytes.length - 4; i++) {
				if (bytes[i] == 0 && bytes[i + 1] == 0 && bytes[i + 2] == 0 && bytes[i + 3] == 0) {
					throw new RuntimeException("Encoded string value contains the end-of-field marker (zero byte).");
				}
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return bytes;
	}
	
	public static byte[] invertBits(byte[] bytes, int startOffset, int endOffset) {
		for (int i = startOffset; i < endOffset; i++) {
			bytes[i] ^= 0xFF;
		}
		return bytes;
	}
}
