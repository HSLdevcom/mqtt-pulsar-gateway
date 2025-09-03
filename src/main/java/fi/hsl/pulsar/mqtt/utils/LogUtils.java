package fi.hsl.pulsar.mqtt.utils;

import org.slf4j.MDC;

import java.util.Map;

public class LogUtils {

    private LogUtils() {

    }

    public static void withFields(Map<String, String> fields, Runnable loggingAction) {
        fields.forEach(MDC::put);
        try {
            loggingAction.run();
        } finally {
            fields.keySet().forEach(MDC::remove);
        }
    }
}
