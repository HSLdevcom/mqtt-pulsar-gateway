package fi.hsl.pulsar.mqtt.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.context.ApplicationContext;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

public class FailFastShutdownTest {

    /** Subclass that intercepts {@link #doExit()} so {@link System#exit} is never called. */
    private static class TestableFailFastShutdown extends FailFastShutdown {
        final CountDownLatch exitLatch = new CountDownLatch(1);
        final AtomicInteger exitCount = new AtomicInteger(0);

        TestableFailFastShutdown(long startTimeMs) {
            super(mock(ApplicationContext.class), startTimeMs);
        }

        @Override
        void doExit() {
            exitCount.incrementAndGet();
            exitLatch.countDown();
        }
    }

    @Test
    @Timeout(5)
    public void secondCallToExitWithFailureIsIgnored() throws InterruptedException {
        // Simulate a start time far enough in the past so there is no grace-period delay.
        long pastStart = System.currentTimeMillis() - FailFastShutdown.STARTUP_GRACE_PERIOD_MS - 1_000;
        TestableFailFastShutdown shutdown = new TestableFailFastShutdown(pastStart);

        shutdown.exitWithFailure(new RuntimeException("first"));
        shutdown.exitWithFailure(new RuntimeException("second"));

        assertTrue(shutdown.exitLatch.await(2, TimeUnit.SECONDS), "shutdown should complete");
        assertEquals(1, shutdown.exitCount.get(), "doExit should be called exactly once");
    }

    @Test
    @Timeout(5)
    public void shutdownFiresPromptlyWhenGracePeriodAlreadyElapsed() throws InterruptedException {
        long pastStart = System.currentTimeMillis() - FailFastShutdown.STARTUP_GRACE_PERIOD_MS - 5_000;
        TestableFailFastShutdown shutdown = new TestableFailFastShutdown(pastStart);

        long before = System.currentTimeMillis();
        shutdown.exitWithFailure(new RuntimeException("late failure"));

        assertTrue(shutdown.exitLatch.await(2, TimeUnit.SECONDS), "shutdown should fire promptly");
        long elapsed = System.currentTimeMillis() - before;
        assertTrue(elapsed < 1_500, "no extra delay expected, but took " + elapsed + "ms");
    }

    @Test
    @Timeout(5)
    public void shutdownFiresImmediatelyWhenGracePeriodSleepIsInterrupted() throws InterruptedException {
        // Start time = now, so the full grace period must elapse. We will interrupt the shutdown
        // thread mid-sleep to verify it still calls doExit() promptly.
        TestableFailFastShutdown shutdown = new TestableFailFastShutdown(System.currentTimeMillis());

        shutdown.exitWithFailure(new RuntimeException("trigger"));

        // Find the FailFastShutdownThread and interrupt it.
        Thread shutdownThread = null;
        long findDeadline = System.currentTimeMillis() + 2_000;
        while (shutdownThread == null && System.currentTimeMillis() < findDeadline) {
            for (Thread t : Thread.getAllStackTraces().keySet()) {
                if ("FailFastShutdownThread".equals(t.getName())) {
                    shutdownThread = t;
                    break;
                }
            }
            if (shutdownThread == null) {
                Thread.sleep(10);
            }
        }
        org.junit.jupiter.api.Assertions.assertNotNull(shutdownThread, "FailFastShutdownThread not found");
        shutdownThread.interrupt();

        // After interruption the thread should fall through and call doExit() immediately.
        assertTrue(shutdown.exitLatch.await(2, TimeUnit.SECONDS), "doExit should be called after interrupt");
        assertEquals(1, shutdown.exitCount.get(), "doExit should be called exactly once");
    }

    @Test
    @Timeout(10)
    public void shutdownIsDelayedUntilGracePeriodWhenCalledEarly() throws InterruptedException {
        // Start time = now, so the full grace period must elapse before doExit() is called.
        // Use a short grace period to keep the test fast: override STARTUP_GRACE_PERIOD_MS is a
        // constant, so we inject a start time only 200 ms ago to get a ~200 ms remaining delay.
        long delayMs = 200;
        long recentStart = System.currentTimeMillis() - (FailFastShutdown.STARTUP_GRACE_PERIOD_MS - delayMs);
        TestableFailFastShutdown shutdown = new TestableFailFastShutdown(recentStart);

        long before = System.currentTimeMillis();
        shutdown.exitWithFailure(new RuntimeException("early failure"));

        // Should NOT fire before the grace period is over.
        boolean firedEarly = shutdown.exitLatch.await(delayMs / 2, TimeUnit.MILLISECONDS);
        assertFalse(firedEarly, "shutdown fired before grace period expired");

        // Should fire shortly after the grace period is over.
        assertTrue(shutdown.exitLatch.await(2_000, TimeUnit.MILLISECONDS), "shutdown did not fire after grace period");
        long elapsed = System.currentTimeMillis() - before;
        assertTrue(elapsed >= delayMs - 20, "elapsed " + elapsed + "ms, expected at least " + delayMs + "ms");
    }
}
