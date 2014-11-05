package com.google.protobuf;  // This is a lie.

/**
 * Helper class to extract byte arrays from {@link ByteString} without copy.
 * <p>
 * Without this protobufs would force us to copy every single byte array out
 * of the objects de-serialized from the wire (which already do one copy, on
 * top of the copies the JVM does to go from kernel buffer to C buffer and
 * from C buffer to JVM buffer).
 *
 * @since 0.96.1
 */
public final class SpliceZeroCopyByteString extends LiteralByteString {
  // Gotten from AsyncHBase code base with permission.
  /** Private constructor so this class cannot be instantiated. */
  private SpliceZeroCopyByteString() {
    super(null);
    throw new UnsupportedOperationException("Should never be here.");
  }

  /**
   * Wraps a byte array in a {@link ByteString} without copying it.
   */
  public static ByteString wrap(final byte[] array) {
    return new LiteralByteString(array);
  }

  /**
   * Wraps a subset of a byte array in a {@link ByteString} without copying it.
   */
  public static ByteString wrap(final byte[] array, int offset, int length) {
    return new BoundedByteString(array, offset, length);
  }

  // TODO:
  // ZeroCopyLiteralByteString.wrap(this.buf, 0, this.count);

  /**
   * Extracts the byte array from the given {@link ByteString} without copy.
   * @param buf A buffer from which to extract the array.  This buffer must be
   * actually an instance of a {@code LiteralByteString}.
   */
  public static byte[] zeroCopyGetBytes(final LiteralByteString buf) {
    return buf.bytes;
  }
}
