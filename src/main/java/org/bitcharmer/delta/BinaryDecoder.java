package org.bitcharmer.delta;

import java.nio.ByteBuffer;

import static org.bitcharmer.delta.BinaryEncoding.META_ELEMENT_BITS;


// Base class for binary decoding. Thread-safe and gc-less. All arguments passed by stack
public abstract class BinaryDecoder<T> {

    private static final int elementSizeMask = (1 << META_ELEMENT_BITS) - 1;

    public void decode(final ByteBuffer buffer, final T array, final boolean ascending) {
        final int length = buffer.get() & 0xFF;
        if (reset(length, 0, array)) return;

        final int bits = buffer.getShort() & 0xFFFF;
        final double divisor = Math.pow(10, bits >>> (META_ELEMENT_BITS));
        final int elementBits = bits & elementSizeMask; // get element width in bits
        populateResult(buffer, array, length, divisor, elementBits, ascending);
    }

    // fill in target (array) with decoded data
    protected abstract void populateResult(ByteBuffer buffer, T array, int length, double divisor, int deltaSize, boolean ascending);

    // decode raw bits into floating-point data
    protected abstract void decodeBits(int idx, long refValue, int length, int deltaSize, long aLong, double divisor,
                                       long mask, int size, ByteBuffer buffer, T array, int sign);

    // set element at idx to its uninitialized/neutral state
    protected abstract void reset(int idx, T t);

    // total size of the underlying data structure
    protected abstract int length(final T array);

    // if actual length matches expected, reset elements from this point onwards (until reaching the last
    // element of the structure)
    protected boolean reset(final int length, final int expected, final T array) {
        if (length == expected) {
            for (int i = length; i < length(array); i++) reset(i, array);
            return true;
        }
        return false;
    }

    // Given remaining bits to decode, read in raw data with the most suitable type, that is: use a type with the
    // closest matching size (equal to or larger then remaining bits. Start with the smallest type (byte) and increase
    // type size to maximize bits read per chunk.
    // Then pass bits read for decoding into floating-point value
    protected void decode(final int idx, final long refValue, final int length, final int deltaSize, final long mask,
                        final ByteBuffer buffer, final T array, double divisor, final int sign) {
        if (idx >= length) return;
        final int remainingBits = (length - idx) * deltaSize;

        if (remainingBits <= Byte.SIZE) decodeBits(idx, refValue, length, deltaSize, buffer.get(), divisor, mask, Byte.SIZE, buffer, array, sign);
        else if (remainingBits <= Short.SIZE) decodeBits(idx, refValue, length, deltaSize, buffer.getShort(), divisor, mask, Short.SIZE, buffer, array, sign);
        else if (remainingBits <= Integer.SIZE) decodeBits(idx, refValue, length, deltaSize, buffer.getInt(), divisor, mask, Integer.SIZE, buffer, array, sign);
        else decodeBits(idx, refValue, length, deltaSize, buffer.getLong(), divisor, mask, Long.SIZE, buffer, array, sign);
    }


}
