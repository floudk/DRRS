package org.apache.flink.streaming.runtime.scale.scheduling;

import java.util.List;
import java.util.TreeSet;

import static org.apache.flink.util.Preconditions.checkArgument;

/* KeySelector inside a subscale
 */
public class NextKeyWrapper {

    protected final TreeSet<Integer> pendingKeys;
    public NextKeyWrapper(List<Integer> pendingKeys) {
        this.pendingKeys = new TreeSet<>(pendingKeys);
    }
    public int getNext(int expected){
        if (expected == -1 ){
            return pendingKeys.pollFirst();
        }
        checkArgument(pendingKeys.contains(expected), "Key %s is not in the pending keys %s", expected, pendingKeys);
        pendingKeys.remove(expected);
        return expected;
    }
    public boolean hasNext(){
        return !pendingKeys.isEmpty();
    }
}
