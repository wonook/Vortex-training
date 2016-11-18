import java.util.*;

public class HashPartitioner<K, V> implements Partitioner<K, V> {
  /**
   * @param keyValues iterator(key, value)
   * @return (destinationIndex -> iterator(key, value))
   */
  @Override
  public Map<Integer, List<KV<K, V>>> partition(Iterator<KV<K, V>> keyValues) {
    // TODO: Perform Hash-partitioning
  }
}
