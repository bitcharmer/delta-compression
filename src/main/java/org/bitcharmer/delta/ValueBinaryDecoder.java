package org.bitcharmer.delta;

import java.nio.ByteBuffer;


public abstract class ValueBinaryDecoder<T> extends BinaryDecoder<T> {

    protected void populateResult(final ByteBuffer buffer, final T array, final int length,
                                  final double divisor, final int elementSize, final boolean ascending) {
        if (elementSize == 0) {
            for (int i = 0; i < length; i++) set(i, 0.0d, array);
            return;
        }
        final long mask = (1L << elementSize) - 1;
        decode(0, 0L, length, elementSize, mask, buffer, array, divisor, 1);
        reset(length, length, array);
    }

    protected void decodeBits(int idx, long refValue, final int length, final int deltaSize, final long bits,
                              final double divisor, final long mask, final int typeSize, final ByteBuffer buffer,
                              final T array, final int sign) {
        for (int offset = typeSize - deltaSize; offset >= 0 && idx < length; offset -= deltaSize)
            set(idx++, (double) ((bits >>> offset) & mask) / divisor, array);
        decode(idx, refValue, length, deltaSize, mask, buffer, array, divisor, sign);
    }

    protected abstract void set(int idx, double value, T t);
}
