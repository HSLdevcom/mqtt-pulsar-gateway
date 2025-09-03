package fi.hsl.pulsar.mqtt.utils;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StructuredLoggingTest {

    private static final Logger log = (Logger) LoggerFactory.getLogger(StructuredLoggingTest.class);

    @Test
    public void testStructuredLoggingWithMDC() {
        ListAppender<ILoggingEvent> listAppender = new ListAppender<>();
        listAppender.start();
        log.addAppender(listAppender);

        Map<String, String> fields = Map.of("inFlight", "42", "userId", "KaladinStormblessed");
        LogUtils.withFields(fields, () -> log.info("Processing messages"));

        List<ILoggingEvent> logsList = listAppender.list;
        assertEquals(1, logsList.size());

        ILoggingEvent logEvent = logsList.get(0);

        assertEquals("Processing messages", logEvent.getFormattedMessage());

        assertEquals("42", logEvent.getMDCPropertyMap().get("inFlight"));
        assertEquals("KaladinStormblessed", logEvent.getMDCPropertyMap().get("userId"));

        listAppender.stop();
    }
}
