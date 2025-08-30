package fi.hsl.pulsar.mqtt.utils;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class BusyWaitTest {
    @Test
    public void testBusyWaitDelay() {
        final long delay = 15000;

        final long start = System.nanoTime();
        BusyWait.delay(delay);
        final long end = System.nanoTime();

        assertTrue(end - start > delay);
    }
}
