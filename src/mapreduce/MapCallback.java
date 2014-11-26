package mapreduce;

import java.util.List;

public interface MapCallback<E, V> {

    public void mapDone(E key, List<V> values);
}
