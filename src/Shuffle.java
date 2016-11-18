import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Shuffle<K, V> {
  final Partitioner<K, V> partitioner;

  Shuffle(final Partitioner<K, V> partitioner) {
    this.partitioner = partitioner;
  }

  public synchronized void write(final Iterator<KV<K, V>> taskOutput) {
    // TODO: Save the data
  }

  public synchronized Iterator<KV<K, V>> read(final int taskIndex) {
    // TODO: Pull the saved data
  }
}
