public class KV<K, V> {
  public final K key;
  public final V value;

  KV(final K key, final V value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public String toString() {
    return '(' + this.key.toString() + ' ' + this.value.toString() + ')';
  }
}
