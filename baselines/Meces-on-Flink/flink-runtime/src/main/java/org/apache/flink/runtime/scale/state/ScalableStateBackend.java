package org.apache.flink.runtime.scale.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.scale.ScalingContext;
import org.apache.flink.runtime.scale.io.message.ScaleBuffer;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.StateMap;

import javax.annotation.Nullable;

import java.util.BitSet;

public interface ScalableStateBackend {

    default void collectOperatorState(){
        throw new UnsupportedOperationException("collectOperatorState is not supported in "
                + this.getClass().getSimpleName());
    }

    default ScaleBuffer.StateBuffer collectKeyedState(KeyOrStateID keyOrStateID) {
        throw new UnsupportedOperationException("collectKeyedState is not supported in "
                + this.getClass().getSimpleName());
    }

    default BitSet mergeKeyedState(ScaleBuffer.StateBuffer sb){
        throw new UnsupportedOperationException("mergeKeyedState is not supported in "
                + this.getClass().getSimpleName());
    }

    default StateMap createStateMap(String stateName, @Nullable BitSet initialBinFlags) {
        throw new UnsupportedOperationException(
                "createStateMap is not supported in " + getClass().getSimpleName());
    }

    default Tuple3<TypeSerializer,TypeSerializer,TypeSerializer> getSerializers(String stateName){
        throw new UnsupportedOperationException(
                "getSerializers is not supported in " + getClass().getSimpleName());
    }

    default StateSnapshotTransformer getStateSnapshotTransformer(String stateName){
       throw new UnsupportedOperationException(
                "getStateSnapshotTransformer is not supported in " + getClass().getSimpleName());
    }

    default ScalingContext.ContextInitInfo getScalingContextInitInfo(){
        throw new UnsupportedOperationException(
                "getScalingContextInitInfo is not supported in " + getClass().getSimpleName());
    }
}
