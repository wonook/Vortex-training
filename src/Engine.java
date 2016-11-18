import java.io.*;
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
    final Shuffle<String, Integer> shuffle;

    // Map
    final CountDownLatch mapWait = new CountDownLatch(numMapTask);
    for (int i = 0 ; i < numMapTask; i ++) {
      final int index = i;
      executors.execute(() -> {
        // TODO: Read inputFile -> Execute Map Task -> Perform Shuffle Write
        // try (final BufferedReader reader = new BufferedReader(new FileReader(inputDirectory+'/'+index))) {
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
        // try (final BufferedWriter writer = new BufferedWriter(new FileWriter(outputDirectory + '/' + index))) {
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
