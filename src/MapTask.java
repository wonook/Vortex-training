public class MapTask implements Task<String, KV<String, Integer>> {
  /**
   * @param line "name count"
   * @return (name, count)
   */
  @Override
  public KV<String, Integer> compute(final String line) {
    // TODO: Parse a line into a Key-Value pair
    String[] tokens = line.split(" ");
    return new KV<String, Integer>(tokens[0], Integer.valueOf(tokens[1]));
  }
}
