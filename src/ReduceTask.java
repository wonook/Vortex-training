import java.util.Iterator;

public class ReduceTask implements Task<KV<String, Iterator<Integer>>, KV<String, Integer>> {
  /**
   * @param groupedValues (name, count1, count2, ...)
   * @return (name, sumOfCounts)
   */
  @Override
  public KV<String, Integer> compute(KV<String, Iterator<Integer>> groupedValues) {
    Iterator<Integer> it = groupedValues.value;
    Integer i = 0;
    while(it.hasNext()) {
      i += it.next();
    }
    return new KV<String, Integer>(groupedValues.key, i);
  }
}
