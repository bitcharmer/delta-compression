package org.bitcharmer.delta;

import java.nio.ByteBuffer;


public abstract class ValueBinaryEncoder<T> extends BinaryEncoder<T> {

    public ValueBinaryEncoder(final int precision) {
        super(precision);
    }

    @Override
    protected void encode(final T array, final int length, final ByteBuffer buffer, final long refValue, final int elementBits, boolean ascending) {
        final int bitsTotal = length * elementBits;
        int bitsWritten = 0;
        int bitsRemaining;
        int idx = 0;

        while ((bitsRemaining = bitsTotal - bitsWritten) > 0) {
            long binary = 0L;
            if (bitsRemaining > Integer.SIZE || elementBits > Integer.SIZE) {
                for (int bitPos = Long.SIZE - elementBits; bitPos >= 0 && bitsWritten < bitsTotal; bitPos -= elementBits, bitsWritten += elementBits)
                    binary |= roundAndPromote(get(idx++, array)) << bitPos;
                buffer.putLong(binary);
            } else if (bitsRemaining > Short.SIZE || elementBits > Short.SIZE) {
                for (int bitPos = Integer.SIZE - elementBits; bitPos >= 0 && bitsWritten < bitsTotal; bitPos -= elementBits, bitsWritten += elementBits)
                    binary |= roundAndPromote(get(idx++, array)) << bitPos;
                buffer.putInt((int) binary);
            } else if (bitsRemaining > Byte.SIZE || elementBits > Byte.SIZE) {
                for (int bitPos = Short.SIZE - elementBits; bitPos >= 0 && bitsWritten < bitsTotal; bitPos -= elementBits, bitsWritten += elementBits)
                    binary |= roundAndPromote(get(idx++, array)) << bitPos;
                buffer.putShort((short) binary);
            }  else {
                for (int bitPos = Byte.SIZE - elementBits; bitPos >= 0 && bitsWritten < bitsTotal; bitPos -= elementBits, bitsWritten += elementBits)
                    binary |= roundAndPromote(get(idx++, array)) << bitPos;
                buffer.put((byte) binary);
            }
        }        
    }

    @Override
    protected int calculateElementWidth(final T array, final int length, final long refValue) {
        long maxValue = 0;
        for (int i = 0; i < length; i++) {
            final long value = roundAndPromote(get(i, array));
            if (value > maxValue) maxValue = value;
        }
        return Long.SIZE - Long.numberOfLeadingZeros(maxValue);
    }

    @Override
    protected final long calculateRefValue(final T array) {
        return 0;
    }

}
