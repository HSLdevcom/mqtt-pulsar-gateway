package fi.hsl.pulsar.mqtt.utils;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

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
