package org.apache.flink.runtime.scale.util;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.io.EOFException;
import java.io.IOException;

public class ByteBufDataOutputView implements DataOutputView {
    private ByteBuf byteBuf;

    public ByteBufDataOutputView(ByteBuf byteBuf) {
        this.byteBuf = byteBuf;
    }


    @Override
    public void skipBytesToWrite(int numBytes) throws IOException {
        if (byteBuf.writableBytes() < numBytes) {
            throw new EOFException("Could not skip " + numBytes + " bytes.");
        }
        byteBuf.writerIndex(byteBuf.writerIndex() + numBytes);
    }

    @Override
    public void write(DataInputView source, int numBytes) throws IOException {
        int bytesRemaining = numBytes;
        while (bytesRemaining > 0) {
            int writableBytes = Math.min(bytesRemaining, byteBuf.writableBytes());
            if (writableBytes == 0) {
                throw new EOFException("Could not write " + numBytes + " bytes. Buffer overflow.");
            }

            byte[] tempBuffer = new byte[writableBytes];
            source.readFully(tempBuffer);
            byteBuf.writeBytes(tempBuffer);
            bytesRemaining -= writableBytes;
        }
    }


    @Override
    public void write(int v) throws IOException {
        byteBuf.writeByte(v);
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        byteBuf.writeBytes(bytes);
    }

    @Override
    public void write(
            byte[] bytes,
            int off,
            int len) throws IOException {
        byteBuf.writeBytes(bytes, off, len);
    }

    @Override
    public void writeBoolean(boolean b) throws IOException {
        byteBuf.writeBoolean(b);
    }

    @Override
    public void writeByte(int i) throws IOException {
        byteBuf.writeByte(i);
    }

    @Override
    public void writeShort(int i) throws IOException {
        byteBuf.writeShort(i);
    }

    @Override
    public void writeChar(int i) throws IOException {
        byteBuf.writeChar(i);
    }

    @Override
    public void writeInt(int i) throws IOException {
        byteBuf.writeInt(i);
    }

    @Override
    public void writeLong(long l) throws IOException {
        byteBuf.writeLong(l);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        byteBuf.writeFloat(v);
    }

    @Override
    public void writeDouble(double v) throws IOException {
        byteBuf.writeDouble(v);
    }

    @Override
    public void writeBytes(String s) throws IOException {
        final int len = s.length();
        if (byteBuf.writableBytes() < len) {
            throw new EOFException("Could not write " + len + " bytes. Buffer overflow.");
        }
        for (int i = 0; i < len; i++) {
            byteBuf.writeByte(s.charAt(i));
        }
    }

    @Override
    public void writeChars(String s) throws IOException {
        final int len = s.length();
        if (byteBuf.writableBytes() < len * 2) {
            throw new EOFException("Could not write " + len + " chars. Buffer overflow.");
        }
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            byteBuf.writeChar(c);
        }
    }

    @Override
    public void writeUTF(String str) throws IOException {
        int strlen = str.length();
        int utflen = 0;
        int c;

        for (int i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                utflen++;
            } else if (c > 0x07FF) {
                utflen += 3;
            } else {
                utflen += 2;
            }
        }

        if (utflen > 65535) {
            throw new IOException("encoded string too long: " + utflen + " bytes");
        }else if (byteBuf.writableBytes() < utflen + 2) {
            throw new EOFException("Could not write " + utflen + " bytes. Buffer overflow.");
        }

        byteBuf.writeShort(utflen);
        for (int i = 0; i < strlen; i++) {
            c = str.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F)) {
                byteBuf.writeByte(c);
            } else if (c > 0x07FF) {
                byteBuf.writeByte(0xE0 | ((c >> 12) & 0x0F));
                byteBuf.writeByte(0x80 | ((c >> 6) & 0x3F));
                byteBuf.writeByte(0x80 | (c & 0x3F));
            } else {
                byteBuf.writeByte(0xC0 | ((c >> 6) & 0x1F));
                byteBuf.writeByte(0x80 | (c & 0x3F));
            }
        }
    }

    public void setPositionUnsafe(int skipBytes) {
        byteBuf.writerIndex(byteBuf.writerIndex() + skipBytes);
    }

    public void writeIntUnsafe(int v, int pos) {
        byteBuf.setInt(pos, v);
    }
}
