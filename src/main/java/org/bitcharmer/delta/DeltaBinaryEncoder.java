package org.bitcharmer.delta;

import java.nio.ByteBuffer;

import static java.lang.Math.abs;


public abstract class DeltaBinaryEncoder<T> extends BinaryEncoder<T> {

    public DeltaBinaryEncoder(final int precision) {
        super(precision);
    }

    protected void encode(final T array, final int length, final ByteBuffer buffer, final long refValue,
                          final int elementBits, boolean ascending) {
        buffer.putLong(refValue);
        if (length == 1) return;

        encodeDeltas(refValue, 1, array, length, buffer, elementBits, ascending);
    }

    protected int calculateElementWidth(final T array, final int length, long refValue) {
        long maxDelta = 0;
        for (int i = 1; i < length; i++) {
            long delta = abs(refValue - (refValue = roundAndPromote(get(i, array))));
            if (delta > maxDelta) maxDelta = delta;
        }
        return Long.SIZE - Long.numberOfLeadingZeros(maxDelta);
    }

    protected long calculateRefValue(final T array) {
        return roundAndPromote(get(0, array));
    }

    protected void encodeDeltas(long ref, int idx, final T array, final int length, final ByteBuffer buffer,
                                int deltaBits, boolean ascending) {
        final int bitsTotal = (length - idx) * deltaBits;
        int bitsWritten = 0;
        int bitsRemaining;
        long ascMask = ascending ? -1 : 0;

        while ((bitsRemaining = bitsTotal - bitsWritten) > 0) {
            long binary = 0L;
            if (bitsRemaining > Integer.SIZE || deltaBits > Integer.SIZE) {
                for (int shift = Long.SIZE - deltaBits; shift >= 0 && bitsWritten < bitsTotal; shift -= deltaBits, bitsWritten += deltaBits) {
                    final long signedDelta = roundAndPromote(get(idx++, array)) - ref;
                    final long unsignedDelta = signedDelta & (-(signedDelta >>> 63) ^ ascMask);
                    binary |=  abs(unsignedDelta) << shift;
                    ref += unsignedDelta;
                }
                buffer.putLong(binary);
            } else {
                for (int shift = Byte.SIZE - deltaBits; shift >= 0 && bitsWritten < bitsTotal; shift -= deltaBits, bitsWritten += deltaBits) {
                    final long signedDelta = roundAndPromote(get(idx++, array)) - ref;
                    final long unsignedDelta = signedDelta & (-(signedDelta >>> 63) ^ ascMask);
                    binary |=  abs(unsignedDelta) << shift;
                    ref += unsignedDelta;
                }
                buffer.put((byte) binary);
            }
        }
    }

}
