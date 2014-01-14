package com.splicemachine.encoding;

import org.apache.hadoop.hbase.util.Bytes;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * @author Scott Fines
 * Created on: 6/7/13
 */
final class DecimalEncoding {

    static final byte[] NULL_DOUBLE_BYTES = new byte[]{0,0};
    static final byte[] NULL_FLOAT_BYTES = new byte[]{0, 0};

    /**
     * Serialize a BigDecimal to a byte[] in such a way as to preserve
     * the natural order of elements.
     *
     * The format for the serialization is as follows:
     *
     * **Summary**
     * The first byte is a header byte, similar to that used to encode longs.
     * The first 2 bits of the header byte are the signum information. The
     * remainder of that byte (plus additional bytes as necessary) will
     * be used to store the <em>adjusted scale</em> of the decimal
     * (described in a bit). The remaining bytes are used to store
     * the <em>Binary Encoded Decimal</em> format of the unscaled integer
     * form. This format uses 4 bits per decimal character, thus
     * requiring 1 byte for every 2 decimal characters in
     * the unscaled integer value(rounded up when
     * there are an odd number of decimal characters.)
     *
     * **Details**
     * BigDecimals are represented by a signed, arbitrary-precision
     * integer (called the <em>unscaled value</em>) and a 32-bit base-2
     * encoded <em>scale</em>. Thus, any big decimal can be represented
     * as {@code (-1)<sup>signum</sup>unscaledValue * 10<sup>-scale</sup>},
     * where <em>signum</em> is either -1,0, or 1 depending on whether
     * the decimal is negative, zero, or positive.
     *
     * Hence, there are three distinct elements which must be stored:
     * {@code signum, unscaled value, } and {@code scale}.
     *
     * To sort negatives before positives, we must first store the signum,
     * which requires a minimum of 2 bits. Because 0x00 is reserved for
     * combinatoric separators, we use the following mapping:
     *
     * -1   = 0x01
     * 0    = 0x02
     * 1    = 0x03
     *
     * After the signum, we store the <em>adjusted scale</em>. Essentially,
     * we are converting all the numbers to be stored into the same base,
     * and recording a new scale. Mathematically, a BigDecimal looks like
     * {@code (-1)<sup>signum</sup>*2<sup>m</sup>*10<sup>-scale</sup>}, where
     * {@code 2<sup>m</sup> } is the base-2 formatted unscaled value.
     * Converting everything into base-10, the unscaled value becomes
     * {@code b*10<sup>-p+1</sup>}, where {@code b} is a scalar and
     * {@code p} is the number of decimal digits in the unscaled
     * value. Thus, combining your exponents nets you
     * the base-10-formatted BigDecimal as
     * {@code (-1)<sup>signum</sup>*b*10<sup>p-scale-1</sup>}. Hence
     * the adjusted scale is {@code p+scale-1}.
     *
     * The adjusted scale requires up to 33 bits of storage if we
     * can use the same format as we use for serializing longs. So, we
     * serialize the adjusted scale using that same format but shifted
     * over by two bits to allow room for the signum
     * (see {@link ScalarEncoding.toBytes(long,boolean))} for more
     * information).
     *
     * Finally, we must serialize the base-10-formatted unscaled value
     * itself. Since that value is arbitrarily long, we use Binary
     * Encoded Decimals to serialize the character digits of the base-10
     * formatted scalar {@code b}.
     *
     * In Binary Encoded Decimals, each decimal character 0-9 is
     * transformed to a 4-bit int between 1-10 (e.g. add 1), and then
     * stored 2 characters to a byte. This means that, if there are {@code
     * N } decimal digits in the unscaled value, there will be {@code N/2}
     * bytes used to store them ({@code N/2 +1} if {@code N} is odd).
     *
     * Thus, the total storage required is 2 bits for the signum, between
     * 6 and 33-bits for the adjusted scale, and {@code N/2} ({@code N/2+1})
     * bytes for the unscaledValue, making the total space at least 1
     * byte.
     *
     * @param value the value to serialize.
     * @param desc whether or not to sort in descending order.
     * @return the byte[] representation of this BigDecimal, in the
     * encoding described above.
     * @throws NullPointerException if {@code value} is null.
     */
    public static byte[] toBytes(BigDecimal value, boolean desc){
        //avoid having duplicate numerically equivalent representations
        value = value.stripTrailingZeros();
        BigInteger i = value.unscaledValue();

        byte[] data;
        if(i==null){
            data = new byte[1];
            byte b = 0x00;
            if(desc)
                b ^= 0xff;
            data[0] = (byte)(b<<Byte.SIZE-2);
            return data;
        }

        if(i.signum()==0){
            data = new byte[1];
            byte b = 0x02;
            if(desc)
                b ^= 0xff;
            data[0] = (byte)((b<<Byte.SIZE-2) &0xff);
            return data;
        }

        int precision = value.precision();
        long exp = precision-value.scale()-1l; //scale <0, so this is precision+scale-1
        /*
         * We need to serialize the exponent using the same format as ScalarEncoding.toBytes(), but
         * with 2 additional bits in the header to describe the signum of the decimal. We use the following
         * mapping for signum values:
         *
         * null ->  0
         * -1   ->  1
         * 0    ->  2
         * 1    ->  3
         *
         */
        byte extraHeader = (byte)(i.signum() <0 ? 0x01:0x03);
        int extraHeaderSize = 2;
        byte[] expBytes = ScalarEncoding.toBytes(exp, extraHeader, extraHeaderSize, desc);
        int expLength = expBytes.length;

        //our string encoding only requires 1 byte for 2 digits
        int length = (precision+1) >>>1;

        data = new byte[expLength + length];
        System.arraycopy(expBytes,0,data,0,expBytes.length);

        String sigString = i.abs().toString(); //strip negatives off if necessary

        char[] sigChars = sigString.toCharArray();
        for(int pos=0;pos<length;pos++){
            byte bcd = 0;
            int strPos = 2*pos;
            if(strPos<precision)
                bcd = (byte)(1+Character.digit(sigChars[strPos],10)<<4);
            strPos++;
            if(strPos<precision)
                bcd |= (byte)(1+Character.digit(sigChars[strPos],10));
            if(desc)
                bcd ^= 0xff;
            data[expLength+pos] = bcd;
        }

        return data;
    }
    public static BigDecimal toBigDecimal(byte[] data, boolean desc){
        return toBigDecimal(data,0,data.length,desc);
    }

    public static BigDecimal toBigDecimal(byte[] data,int dataOffset,int dataLength, boolean desc){
        int h = data[dataOffset];
        if(desc)
            h ^=0xff;
        h &=0xff;
        h >>>=Byte.SIZE-2;
        if(h==0x00) return null;
        if(h==0x02) return BigDecimal.ZERO;

        byte sign = (byte)(h==0x01 ? -1:0);
        long[] expOffset = ScalarEncoding.toLongWithOffset(data, dataOffset,2, desc);
        long exp =  expOffset[0];
        int offset = (int)(expOffset[1]);

        int length=((dataLength-offset)*2);
        byte last;
        if(dataOffset+dataLength>=data.length)
            last = data[data.length-1];
        else
            last = data[dataOffset+dataLength-1];
        if(desc)
            last ^= 0xff;
        if((last &0xf) ==0)
            length-=1;

        //deserialize the digits
        char[] chars = new char[length];
        int pos=0;
        for(int i=0;i<dataLength && pos<chars.length;i++){
//            if(pos>chars.length) break; //we're done
            byte next = data[dataOffset+offset+i];
            if(desc)
                next ^=0xff;
            byte f = (byte)((next>>>4) & 0xf);
            if(f==0)
                break; //no more character
            else{
                chars[pos] = (char)('0'+f-1);
                pos++;
            }
            f = (byte)(next&0xf);
            if(f==0)
                break;
            else{
                chars[pos] = (char)('0'+f-1);
                pos++;
            }
        }

        int scale = (int)(exp-length+1l);
        BigInteger i = new BigInteger(sign==0?new String(chars): '-'+new String(chars));
        return new BigDecimal(i,-scale);
    }

    /**
     * Will generate an 8-byte, big-endian, sorted representation of the Double, in accordance
     * with IEEE 754, except that all NaNs will be coalesced into a single "canonical" NaN.
     *
     * Because it is not possible to
     *
     * The returned byte[] will <em>never</em> encode to all zeros. This makes an 8-byte zero field
     * available to use as a NULL indicator.
     *
     * @param value the double to encode
     * @param desc whether or not to encode in descending order
     * @return an 8-byte, big-endian, sorted encoding of the double.
     */
    public static byte[] toBytes(double value, boolean desc){
        long l = Double.doubleToLongBits(value);
        l = (l^ ((l >> Long.SIZE-1) | Long.MIN_VALUE))+1;

        byte[] bytes = Bytes.toBytes(l);
        if(desc) {
        	for(int i=0;i<bytes.length;i++){
        		bytes[i] ^= 0xff;
        	}
        }
        return bytes;
    }

    public static double toDouble(byte[] data, boolean desc){
        return toDouble(data, 0, desc);
    }

    public static double toDouble(ByteBuffer data,boolean desc){
        long l = data.asLongBuffer().get();
        if(desc)
            l ^= 0xffffffffffffffffl;

        l--;
        l ^= (~l >> Long.SIZE-1) | Long.MIN_VALUE;
        return Double.longBitsToDouble(l);
    }

    public static double toDouble(byte[] data, int offset,boolean desc){
    	byte[] val = data;
    	if(desc){
    		val = new byte[8];
    		System.arraycopy(data,offset,val,0,val.length);
    		for(int i=0;i<8;i++){
    			val[i] ^= 0xff;
    		}
    		offset=0;
    	}
    		
        long l = Bytes.toLong(val,offset);

        l--;
        l ^= (~l >> Long.SIZE-1) | Long.MIN_VALUE;
        return Double.longBitsToDouble(l);
    }

    public static byte[] toBytes(float value,boolean desc){

        int j = Float.floatToIntBits(value);
        j = (j^((j>>Integer.SIZE-1) | Integer.MIN_VALUE))+1;

        if(desc)
            j^=0xffffffff;

        return Bytes.toBytes(j);
    }

    public static float toFloat(byte[] data, boolean desc){
        return toFloat(data,0,desc);
    }

    public static float toFloat(ByteBuffer data, boolean desc){
        int j = data.asIntBuffer().get();
        if(desc)
            j ^= 0xffffffff;

        j--;
        j ^= (~j >> Integer.SIZE-1)|Integer.MIN_VALUE;

        return Float.intBitsToFloat(j);
    }

    public static float toFloat(byte[] data, int offset,boolean desc){
        int j = Bytes.toInt(data,offset);
        if(desc)
            j ^= 0xffffffff;

        j--;
        j ^= (~j >> Integer.SIZE-1)|Integer.MIN_VALUE;

        return Float.intBitsToFloat(j);
    }

    public static void main(String... args) throws Exception{
        byte b = 0x02;
        System.out.printf("%8s%n",Integer.toBinaryString(b));
        b <<= 6;
        System.out.printf("%8s%n",Integer.toBinaryString(b));
        b &= 0xff;
        System.out.printf("%8s%n",Integer.toBinaryString(b));

    }
}
