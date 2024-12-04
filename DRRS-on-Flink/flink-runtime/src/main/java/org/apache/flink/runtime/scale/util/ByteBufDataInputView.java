package org.apache.flink.runtime.scale.util;

import org.apache.flink.core.memory.DataInputView;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.io.UTFDataFormatException;

public class ByteBufDataInputView implements DataInputView {

    ByteBuf byteBuf;

    public ByteBufDataInputView(ByteBuf byteBuf) {
        this.byteBuf = byteBuf;
    }


    public int remaining() {
        return byteBuf.readableBytes();
    }

    @Override
    public void skipBytesToRead(int numBytes) throws IOException {
        byteBuf.skipBytes(numBytes);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException("Byte array b cannot be null.");
        }

        if (off < 0) {
            throw new IllegalArgumentException("The offset off cannot be negative.");
        }

        if (len < 0) {
            throw new IllegalArgumentException("The length len cannot be negative.");
        }
        int toRead = Math.min(len, byteBuf.readableBytes());
        byteBuf.readBytes(b, off, toRead);
        return toRead;
    }


    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public void readFully(byte[] bytes) throws IOException {
        byteBuf.readBytes(bytes);
    }

    @Override
    public void readFully(
            byte[] bytes,
            int off,
            int len) throws IOException {
        if (off < 0 || len < 0 || off > bytes.length - len) {
            throw new IndexOutOfBoundsException();
        }
        byteBuf.readBytes(bytes, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        if (n < 0) {
            throw new IllegalArgumentException();
        }
        int toSkip = Math.min(n, byteBuf.readableBytes());
        byteBuf.skipBytes(toSkip);
        return toSkip;
    }

    @Override
    public boolean readBoolean() throws IOException {
        return byteBuf.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        return byteBuf.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return byteBuf.readUnsignedByte();
    }

    @Override
    public short readShort() throws IOException {
        return byteBuf.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return byteBuf.readUnsignedShort();
    }

    @Override
    public char readChar() throws IOException {
        return byteBuf.readChar();
    }

    @Override
    public int readInt() throws IOException {
        return byteBuf.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return byteBuf.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        return byteBuf.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        return byteBuf.readDouble();
    }

    @Override
    public String readLine() throws IOException {
        final StringBuilder bld = new StringBuilder(32);
        int b;
        while ((b = readUnsignedByte()) != '\n') {
            if (b != '\r') {
                bld.append((char) b);
            }
        }
        if (bld.length() == 0) {
            return null;
        }
        int len = bld.length();
        if (len > 0 && bld.charAt(len - 1) == '\r') {
            bld.setLength(len - 1);
        }
        return bld.toString();
    }

    private byte[] utfByteBuffer; // reusable byte buffer for utf-8 decoding
    private char[] utfCharBuffer; // reusable char buffer for utf-8 decoding

    @Override
    public String readUTF() throws IOException {
        final int utflen = readUnsignedShort();

        final byte[] bytearr;
        final char[] chararr;

        if (this.utfByteBuffer == null || this.utfByteBuffer.length < utflen) {
            bytearr = new byte[utflen];
            this.utfByteBuffer = bytearr;
        } else {
            bytearr = this.utfByteBuffer;
        }
        if (this.utfCharBuffer == null || this.utfCharBuffer.length < utflen) {
            chararr = new char[utflen];
            this.utfCharBuffer = chararr;
        } else {
            chararr = this.utfCharBuffer;
        }

        int c, char2, char3;
        int count = 0;
        int chararrCount = 0;

        readFully(bytearr, 0, utflen);

        while (count < utflen) {
            c = (int) bytearr[count] & 0xff;
            if (c > 127) {
                break;
            }
            count++;
            chararr[chararrCount++] = (char) c;
        }

        while (count < utflen) {
            c = (int) bytearr[count] & 0xff;
            switch (c >> 4) {
                case 0:
                case 1:
                case 2:
                case 3:
                case 4:
                case 5:
                case 6:
                case 7:
                    count++;
                    chararr[chararrCount++] = (char) c;
                    break;
                case 12:
                case 13:
                    count += 2;
                    if (count > utflen) {
                        throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                    }
                    char2 = bytearr[count - 1];
                    if ((char2 & 0xC0) != 0x80) {
                        throw new UTFDataFormatException("malformed input around byte " + count);
                    }
                    chararr[chararrCount++] = (char) (((c & 0x1F) << 6) | (char2 & 0x3F));
                    break;
                case 14:
                    count += 3;
                    if (count > utflen) {
                        throw new UTFDataFormatException(
                                "malformed input: partial character at end");
                    }
                    char2 = bytearr[count - 2];
                    char3 = bytearr[count - 1];
                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
                        throw new UTFDataFormatException(
                                "malformed input around byte " + (count - 1));
                    }
                    chararr[chararrCount++] =
                            (char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | (char3 & 0x3F));
                    break;
                default:
                    throw new UTFDataFormatException("malformed input around byte " + count);
            }
        }
        // The number of chars produced may be less than utflen
        return new String(chararr, 0, chararrCount);
    }

}
