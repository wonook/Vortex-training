import java.io.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Engine {
  final ExecutorService executors = Executors.newFixedThreadPool(2);
  final String inputDirectory;
  final int numMapTask;
  final int numReduceTask;
  final String outputDirectory;

  Engine(final String inputDirectory,
         final int numMapTask,
         final int numReduceTask,
         final String outputDirectory) {
    this.inputDirectory = inputDirectory;
    this.numMapTask = numMapTask;
    this.numReduceTask = numReduceTask;
    this.outputDirectory = outputDirectory;
  }

  void runJob() throws InterruptedException {
    // Shuffle Configuration
    final Shuffle<String, Integer> shuffle = new Shuffle<>(new HashPartitioner<String, Integer>());

    // Map
    final CountDownLatch mapWait = new CountDownLatch(numMapTask);
    for (int i = 0 ; i < numMapTask; i ++) {
      final int index = i;
      executors.execute(() -> {
        // TODO: Read inputFile -> Execute Map Task -> Perform Shuffle Write
        MapTask mapper = new MapTask();
        ArrayList<KV<String, Integer>> mapped = new ArrayList<>();
        try (final BufferedReader reader = new BufferedReader(new FileReader(inputDirectory+'/'+index))) {
          String line = null;
          while ((line = reader.readLine()) != null) {
            mapped.add(mapper.compute(line));
          }
        } catch (Exception e) {
        }
        System.out.println(mapped);
        shuffle.write(mapped);
        mapWait.countDown();
      });
    }
    mapWait.await();

    // Reduce
    final CountDownLatch reduceWait = new CountDownLatch(numReduceTask);
    for (int i = 0 ; i < numReduceTask; i++) {
      final int index = i;
      executors.execute(() -> {
        // TODO: Perform Shuffle Read -> Execute Reduce Task -> Write outputFile
        Iterator<KV<String, Integer>> it = shuffle.read(index);
        ReduceTask reducer = new ReduceTask();
        HashMap<String, List<Integer>> map = new HashMap<>();
        while(it != null && it.hasNext()) {
          KV<String, Integer> kv = it.next();
          List<Integer> list = map.get(kv.key);
          if (list == null) list = new ArrayList<>();
          list.add(kv.value);
          map.put(kv.key, list);
        }
        System.out.println("EntrySEt:" + map.entrySet());

        try (final BufferedWriter writer = new BufferedWriter(new FileWriter(outputDirectory + '/' + index))) {
          for (Map.Entry<String, List<Integer>> e : map.entrySet()) {
            writer.write(reducer.compute(new KV<String, Iterator<Integer>>(e.getKey(), e.getValue().iterator())).toString() + "\n");
          }
        } catch (Exception e) {
        }
        reduceWait.countDown();
      });
    }
    reduceWait.await();
  }

  public static void main(final String[] args) throws IOException, InterruptedException {
    new Engine(args[0], Integer.valueOf(args[1]), Integer.valueOf(args[2]), args[3]).runJob();
    System.exit(0);
  }
}
