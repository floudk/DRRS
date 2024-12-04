package org.apache.flink.runtime.scale.io.message.barrier;

import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

/**
 * Barrier to signal that all subsequent data is in new routing table,
 * and also a signal to show the scale in current channel is finished.
 * <p>
 * This barrier is only prior in the subpartition but not in the input channel.
 */

public class ConfirmBarrier extends ScaleBarrier {
    private static final long serialVersionUID = 1L;

    private final Set<Integer> involvedKeys;
    public final int subscaleID;

    public ConfirmBarrier(Set<Integer> involvedKeys, int subscaleID) {
        super();
        this.involvedKeys = new HashSet<>(involvedKeys);
        this.subscaleID = subscaleID;
    }

    public Set<Integer> getInvolvedKeys() {
        return involvedKeys;
    }


    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ConfirmBarrier) {
            ConfirmBarrier other = (ConfirmBarrier) obj;
            return subscaleID == other.subscaleID;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return subscaleID;
    }

    @Override
    public String toString() {
        return "CB(" + subscaleID + ")";
    }


    public static ByteBuffer serializeConfirmBarrier(ConfirmBarrier barrier) throws IOException {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bos)) {
            out.writeObject(barrier);
            out.flush();
        }

        int capacity = 4 + bos.size(); // barrier type(4) + serialized object
        ByteBuffer buf = ByteBuffer.allocate(capacity);

        buf.putInt(EventSerializer.SCALE_CONFIRM_EVENT); // barrier type
        buf.put(bos.toByteArray()); // serialized object
        buf.flip();
        return buf;

    }
    public static ConfirmBarrier deserializeConfirmBarrier(ByteBuffer buffer)throws IOException  {
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);
        ByteArrayInputStream bis = new ByteArrayInputStream(data);
        try (ObjectInputStream in = new ObjectInputStream(bis)) {
            return (ConfirmBarrier) in.readObject();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
