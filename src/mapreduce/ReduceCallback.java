package mapreduce;

import java.util.Map;

public interface ReduceCallback<E, K, V> {

    public void reduceDone(E e, Map<K, V> results);
}
