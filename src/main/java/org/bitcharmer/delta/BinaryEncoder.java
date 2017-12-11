package org.bitcharmer.delta;

import java.nio.ByteBuffer;

import static org.bitcharmer.delta.BinaryEncoding.META_ELEMENT_BITS;
import static org.bitcharmer.delta.BinaryEncoding.META_LENGTH_BITS;
import static org.bitcharmer.delta.BinaryEncoding.META_PRECISION_BITS;


// Base class for binary encoding. Thread-safe and gc-less. All arguments passed by stack
public abstract class BinaryEncoder<T> {

    public static final int SUPPORTED_LENGTH = (1 << META_LENGTH_BITS) - 1;
    public static final int SUPPORTED_PRECISION = (1 << META_PRECISION_BITS) - 1;
    public static final int SUPPORTED_ELEMENT_SIZE_BITS = (1 << META_ELEMENT_BITS) - 1;
    public static final long MAX_SUPPORTED_INTEGER_PART = 10_000_000L;

    protected final int precision;
    protected final int multiplier;
    protected final long maxSupportedDelta;

    public BinaryEncoder(final int precision) {
        precisionCheck(precision);
        this.precision = precision;
        this.multiplier = (int) Math.pow(10, precision);
        this.maxSupportedDelta = Long.MAX_VALUE / multiplier;
    }

    public void encode(final T array, final int length, final ByteBuffer buffer, boolean ascending) {
        lengthCheck(length);
        buffer.put((byte) length);
        if (length == 0) return;

        final long refValue = calculateRefValue(array);
        final int elementBits = calculateElementWidth(array, length, refValue);
        elementSizeCheck(elementBits);
        buffer.putShort((short) (precision << META_ELEMENT_BITS | elementBits));

        encode(array, length, buffer, refValue, elementBits, ascending);
    }

    protected abstract void encode(T array, int length, ByteBuffer buffer, long refValue, int elementBits, boolean ascending);
    protected abstract int calculateElementWidth(T array, int length, long refValue);
    protected abstract long calculateRefValue(T array);
    protected abstract double get(int idx, T array);

    protected void precisionCheck(final int precision) {
        if (precision > SUPPORTED_PRECISION) throw new IllegalArgumentException("Maximum supported precision is " + SUPPORTED_PRECISION);
    }

    protected void lengthCheck(final int length) {
        if (length > SUPPORTED_LENGTH) throw new IllegalArgumentException("Maximum supported length is " + SUPPORTED_LENGTH);
        if (length < 0) throw new IllegalArgumentException("Illegal declared array length: " + length);
    }

    protected void elementSizeCheck(final int deltaBits) {
        if (deltaBits > SUPPORTED_ELEMENT_SIZE_BITS) throw new IllegalArgumentException("Maximum supported delta bits is " + META_ELEMENT_BITS);
    }

    protected long roundAndPromote(final double value) {
        overflowCheck(value);
        return (long) (value * multiplier + .5d);
    }

    protected void overflowCheck(final double value) {
        final long longValue = (long) value;
        if (longValue > maxSupportedDelta) throw new IllegalArgumentException("Overflow detected. Value too high: " + value);
        if (longValue > MAX_SUPPORTED_INTEGER_PART) throw new IllegalArgumentException("Integer part too high. Would result in precision loss: " + value);
    }

}