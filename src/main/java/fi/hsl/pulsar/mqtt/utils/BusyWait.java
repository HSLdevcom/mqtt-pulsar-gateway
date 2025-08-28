package fi.hsl.pulsar.mqtt.utils;

public class BusyWait {
  private BusyWait() {}

  public static void delay(long ns) {
    final long start = System.nanoTime();
    while (System.nanoTime() - start < ns)
      ;
  }
}
