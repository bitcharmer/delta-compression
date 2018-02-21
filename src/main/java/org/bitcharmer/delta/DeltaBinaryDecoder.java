package org.bitcharmer.delta;

import java.nio.ByteBuffer;


public abstract class DeltaBinaryDecoder<T> extends BinaryDecoder<T> {

    protected void populateResult(final ByteBuffer buffer, final T array, final int length,
                                  final double divisor, final int deltaSize, final boolean ascending) {
        final long longRef = buffer.getLong();
        final double doubleValue = longRef / divisor;
        set(0, doubleValue, array);
        if (reset(length, 1, array)) return;

        if (deltaSize == 0) {
            for (int i = 1; i < length; i++) set(i, doubleValue, array);
        } else {
            final long mask = (1L << deltaSize) - 1;
            final int sign = ascending ? 1 : -1;
            decode(1, longRef, length, deltaSize, mask, buffer, array, divisor, sign);
        }
        reset(length, length, array);
    }

    protected void decodeBits(int idx, long refValue, final int length, final int deltaSize, final long bits,
                              final double divisor, final long mask, final int typeSize, final ByteBuffer buffer,
                              final T array, final int sign) {
        for (int offset = typeSize - deltaSize; offset >= 0 && idx < length; offset -= deltaSize)
            set(idx++, (refValue += (((bits >>> offset) & mask)) * sign) / divisor, array);
        decode(idx, refValue, length, deltaSize, mask, buffer, array, divisor, sign);
    }

    protected abstract void set(int idx, double value, T t);

}
