package fi.hsl.pulsar.mqtt.config;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PulsarPropertiesTest {

    private static final Validator validator;

    static {
        try (ValidatorFactory factory = Validation.buildDefaultValidatorFactory()) {
            validator = factory.getValidator();
        }
    }

    @Test
    public void validPropertiesPassValidation() {
        PulsarProperties props = new PulsarProperties("pulsar://localhost:6650", "mqtt-raw", 20, 10_000);

        Set<ConstraintViolation<PulsarProperties>> violations = validator.validate(props);

        assertTrue(violations.isEmpty());
        assertEquals("pulsar://localhost:6650", props.serviceUrl());
        assertEquals("mqtt-raw", props.topic());
        assertEquals(20, props.sendTimeoutSeconds());
        assertEquals(10_000, props.maxPendingMessages());
    }

    @Test
    public void rejectsBlankServiceUrl() {
        PulsarProperties props = new PulsarProperties(" ", "mqtt-raw", 20, 10_000);

        Set<ConstraintViolation<PulsarProperties>> violations = validator.validate(props);

        assertFalse(violations.isEmpty());
    }

    @Test
    public void rejectsBlankTopic() {
        PulsarProperties props = new PulsarProperties("pulsar://localhost:6650", "", 20, 10_000);

        Set<ConstraintViolation<PulsarProperties>> violations = validator.validate(props);

        assertFalse(violations.isEmpty());
    }

    @Test
    public void rejectsNonPositiveSendTimeout() {
        PulsarProperties props = new PulsarProperties("pulsar://localhost:6650", "mqtt-raw", 0, 10_000);

        Set<ConstraintViolation<PulsarProperties>> violations = validator.validate(props);

        assertFalse(violations.isEmpty());
    }

    @Test
    public void rejectsNonPositiveMaxPendingMessages() {
        PulsarProperties props = new PulsarProperties("pulsar://localhost:6650", "mqtt-raw", 20, -1);

        Set<ConstraintViolation<PulsarProperties>> violations = validator.validate(props);

        assertFalse(violations.isEmpty());
    }
}
