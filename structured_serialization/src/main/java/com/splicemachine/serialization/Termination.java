package com.gotometrics.orderly;

/**
 * <h1> Termination </h1>
 * Some row keys, such as character strings, require an explicit termination
 * byte during serialization to indicate the end of the serialized value.
 * This terminator byte can be omitted in some situations, such as during an
 * ascending sort where the only serialized bytes come from the string row key.
 * Omitting the explicit terminator byte is known as implicit termination,
 * because the end of the serialized byte array implicitly terminates the
 * serialized value. The {@link RowKey#setTermination} method can be used to
 * control when termination is required.
 *
 * <p>If a row key is not forced to terminate, then during deserialization it
 * will read bytes up until the end of the serialized byte array. This is safe
 * if the row key serialized all of the bytes up to the end of the byte array
 * (which is the common case). However, if the user has created a custom
 * serialized format where their own extra bytes are appended to the byte array,
 * then this would produce incorrect results and explicit termination should
 * be forced.</p>
 *
 * <p>The JavaDoc of each
 * row key class describes the effects of implicit and explicit termination
 * of the class's serialization. Note that the <code>termination</code> flag
 * only affects serialization. For all row key types, deserialization and skip
 * methods are able to detect values encoded in both implicit and explicit
 * terminated formats, regardless of what the <code>termination</code> flag
 * is set to.</p>
 *
 * <p>There are three possible values for the <code>mustTerminate</code> flag: AUTO, MUST or SHOULD_NOT. AUTO will only
 * use termination if really necessary, MUST always writes termination bytes and SHOULD_NOT never writes them. Using
 * SHOULD_NOT implies you have to know what you are doing! This can result in ambiguous rowkeys.</p>
 *
 * <p>Note that SHOULD_NOT is stronger than MUST.</p>
 *
 * @author Jan Van Besien
 */
public enum Termination {
    AUTO, MUST, SHOULD_NOT
}
