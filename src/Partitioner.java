import java.util.Iterator;
import java.util.List;
import java.util.Map;

public interface Partitioner<K, V> {
  Map<K, Integer> partition(Iterator<KV<K, V>> keyValues);
}
