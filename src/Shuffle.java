import java.util.HashMap;
import java.util.*;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Shuffle<K, V> {
  final Partitioner<K, V> partitioner;
  private Map<Integer, List<KV<K, V>>> file = new HashMap<>();

  Shuffle(final Partitioner<K, V> partitioner) {
    this.partitioner = partitioner;
  }

  public synchronized void write(final List<KV<K, V>> taskOutput) {
    // TODO: Save the data
    Map<K, Integer> map = partitioner.partition(taskOutput.iterator());
    for (KV<K, V> e : taskOutput) {
      System.out.println(e.toString());
      List<KV<K, V>> list = file.get(map.get(e.key));
      if (list == null) list = new ArrayList<KV<K, V>>();
      list.add(e);
      file.put(map.get(e.key), list);
    }
    System.out.println(file);
    return;
  }

  public synchronized Iterator<KV<K, V>> read(final int taskIndex) {
    // TODO: Pull the saved data
    List<KV<K, V>> data = file.get(taskIndex);
    if (data == null) data = new ArrayList<>();
    System.out.println(data);
    return data.iterator();
  }
}
