package org.bitcharmer.delta;

import java.nio.ByteBuffer;

import static java.lang.Math.abs;

/**
 * Created by kudlaw on 02/06/14.
 */
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
        long ascMask = ascending ? -1 : 0; // all 1s if ascending, all 0s if descending

        while ((bitsRemaining = bitsTotal - bitsWritten) > 0) {
            long binary = 0L;
            if (bitsRemaining > Integer.SIZE || deltaBits > Integer.SIZE) {
                for (int shift = Long.SIZE - deltaBits; shift >= 0 && bitsWritten < bitsTotal; shift -= deltaBits, bitsWritten += deltaBits) {
                    long val = roundAndPromote(get(idx++, array));
                    long delta = val - ref;
                    long deltaMask = -(delta >>> 63);  // all 0s if delta positive
                    long fullMask = deltaMask ^ ascMask;  // gives 1s for +delta asc, -delta desc, else 0s
                    // the above is equivalent to: if( (delta<0 && ascending) || (delta>=0 && !ascending) )   delta = 0;
                    long lossyDelta = delta & fullMask;
                    binary |=  abs(lossyDelta) << shift;
                    ref += lossyDelta;
                }
                buffer.putLong(binary);
            } else if (bitsRemaining > Short.SIZE || deltaBits > Short.SIZE) {
                for (int shift = Integer.SIZE - deltaBits; shift >= 0 && bitsWritten < bitsTotal; shift -= deltaBits, bitsWritten += deltaBits) {
                    long val = roundAndPromote(get(idx++, array));
                    long delta = val - ref;
                    long deltaMask = -(delta >>> 63);  // all 0s if delta positive
                    long fullMask = deltaMask ^ ascMask;  // gives 1s for +delta asc, -delta desc, else 0s
                    // the above is equivalent to: if( (delta<0 && ascending) || (delta>=0 && !ascending) )   delta = 0;
                    long lossyDelta = delta & fullMask;
                    binary |=  abs(lossyDelta) << shift;
                    ref += lossyDelta;
                }
                buffer.putInt((int) binary);
            } else if (bitsRemaining > Byte.SIZE || deltaBits > Byte.SIZE) {
                for (int shift = Short.SIZE - deltaBits; shift >= 0 && bitsWritten < bitsTotal; shift -= deltaBits, bitsWritten += deltaBits) {
                    long val = roundAndPromote(get(idx++, array));
                    long delta = val - ref;
                    long deltaMask = -(delta >>> 63);  // all 0s if delta positive
                    long fullMask = deltaMask ^ ascMask;  // gives 1s for +delta asc, -delta desc, else 0s
                    // the above is equivalent to: if( (delta<0 && ascending) || (delta>=0 && !ascending) )   delta = 0;
                    long lossyDelta = delta & fullMask;
                    binary |=  abs(lossyDelta) << shift;
                    ref += lossyDelta;
                }
                buffer.putShort((short) binary);
            }  else {
                for (int shift = Byte.SIZE - deltaBits; shift >= 0 && bitsWritten < bitsTotal; shift -= deltaBits, bitsWritten += deltaBits) {
                    long val = roundAndPromote(get(idx++, array));
                    long delta = val - ref;
                    long deltaMask = -(delta >>> 63);  // all 0s if delta positive
                    long fullMask = deltaMask ^ ascMask;  // gives 1s for +delta asc, -delta desc, else 0s
                    // the above is equivalent to: if( (delta<0 && ascending) || (delta>=0 && !ascending) )   delta = 0;
                    long lossyDelta = delta & fullMask;
                    binary |=  abs(lossyDelta) << shift;
                    ref += lossyDelta;
                }
                buffer.put((byte) binary);
            }
        }
    }

}
