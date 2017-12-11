package org.bitcharmer.delta;

public interface BinaryEncoding {

    /*
        Header definition for binary-encoded array of floating point numbers

        Field                   Size in bits      Type            Description
        -------------------------------------------------------------------------------------------------
        Array length            8                 byte            Number of encoded elements
        Precision               4                 1 nibble        Number of decimal places used
        Element size in bits    12                3 nibbles      Number of bits to store a single element
        Reference value         64                long            Optional - only for delta encoding

     */

    public static final int META_LENGTH_BITS = 8;
    public static final int META_PRECISION_BITS = 4;
    public static final int META_ELEMENT_BITS = 12;

}
