package org.apache.flink.runtime.scale.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.scale.io.message.local.StateBuffer;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.StateMap;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface ScalableStateBackend {

    default void collectOperatorState(){
        throw new UnsupportedOperationException("collectOperatorState is not supported in "
                + this.getClass().getSimpleName());
    }

    default void collectKeyedState(
            List<Integer> keyGroups,
            StateBuffer migratingStates) throws IOException {
        throw new UnsupportedOperationException("collectKeyedState is not supported in "
                + this.getClass().getSimpleName());
    }

    default void mergeKeyedState(StateBuffer sb){
        throw new UnsupportedOperationException("mergeKeyedState is not supported in "
                + this.getClass().getSimpleName());
    }

    default StateMap createStateMap(String stateName) {
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

    default Map<Integer, Long> getStateSizes(){
        throw new UnsupportedOperationException(
                "getStateSizes is not supported in " + getClass().getSimpleName());
    }
}
