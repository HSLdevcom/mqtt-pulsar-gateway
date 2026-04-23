package fi.hsl.pulsar.mqtt.config;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MqttPropertiesTest {

    private static final Validator validator;

    static {
        try (ValidatorFactory factory = Validation.buildDefaultValidatorFactory()) {
            validator = factory.getValidator();
        }
    }

    @Test
    public void validPropertiesWithCredentialsPassValidation() {
        MqttProperties props = new MqttProperties("tcp://localhost:1883", "test/#", 1, "cid", true, 10_000, 30, 10, "u",
                "p");

        Set<ConstraintViolation<MqttProperties>> violations = validator.validate(props);

        assertTrue(violations.isEmpty());
        assertEquals("tcp://localhost:1883", props.brokerUrl());
        assertEquals("test/#", props.topic());
        assertEquals(1, props.qos());
        assertEquals("cid", props.clientId());
        assertTrue(props.cleanSession());
        assertEquals(10_000, props.maxInflight());
        assertEquals(30, props.keepAliveIntervalSeconds());
        assertEquals(10, props.connectionTimeoutSeconds());

        Optional<MqttProperties.Credentials> credentials = props.credentials();
        assertTrue(credentials.isPresent());
        assertEquals("u", credentials.get().username());
        assertEquals("p", credentials.get().password());
    }

    @Test
    public void validPropertiesWithoutCredentialsPassValidation() {
        MqttProperties props = new MqttProperties("tcp://localhost:1883", "test/#", 1, "cid", true, 10_000, 30, 10,
                null, null);

        Set<ConstraintViolation<MqttProperties>> violations = validator.validate(props);

        assertTrue(violations.isEmpty());
        assertTrue(props.credentials().isEmpty());
    }

    @Test
    public void emptyUsernameAndPasswordTreatedAsAbsent() {
        MqttProperties props = new MqttProperties("tcp://localhost:1883", "test/#", 1, "cid", true, 10_000, 30, 10, "",
                "");

        Set<ConstraintViolation<MqttProperties>> violations = validator.validate(props);

        assertTrue(violations.isEmpty());
        assertTrue(props.credentials().isEmpty());
    }

    @Test
    public void rejectsUsernameWithoutPassword() {
        MqttProperties props = new MqttProperties("tcp://localhost:1883", "test/#", 1, "cid", true, 10_000, 30, 10,
                "user", null);

        Set<ConstraintViolation<MqttProperties>> violations = validator.validate(props);

        assertFalse(violations.isEmpty());
    }

    @Test
    public void rejectsPasswordWithoutUsername() {
        MqttProperties props = new MqttProperties("tcp://localhost:1883", "test/#", 1, "cid", true, 10_000, 30, 10,
                null, "secret");

        Set<ConstraintViolation<MqttProperties>> violations = validator.validate(props);

        assertFalse(violations.isEmpty());
    }

    @Test
    public void rejectsBlankBrokerUrl() {
        MqttProperties props = new MqttProperties(" ", "test/#", 1, "cid", true, 10_000, 30, 10, null, null);

        Set<ConstraintViolation<MqttProperties>> violations = validator.validate(props);

        assertFalse(violations.isEmpty());
    }

    @Test
    public void rejectsBlankTopic() {
        MqttProperties props = new MqttProperties("tcp://localhost:1883", " ", 1, "cid", true, 10_000, 30, 10, null,
                null);

        Set<ConstraintViolation<MqttProperties>> violations = validator.validate(props);

        assertFalse(violations.isEmpty());
    }

    @Test
    public void rejectsBlankClientId() {
        MqttProperties props = new MqttProperties("tcp://localhost:1883", "test/#", 1, "", true, 10_000, 30, 10, null,
                null);

        Set<ConstraintViolation<MqttProperties>> violations = validator.validate(props);

        assertFalse(violations.isEmpty());
    }

    @Test
    public void rejectsQosBelow0() {
        MqttProperties props = new MqttProperties("tcp://localhost:1883", "test/#", -1, "cid", true, 10_000, 30, 10,
                null, null);

        Set<ConstraintViolation<MqttProperties>> violations = validator.validate(props);

        assertFalse(violations.isEmpty());
    }

    @Test
    public void rejectsQosAbove2() {
        MqttProperties props = new MqttProperties("tcp://localhost:1883", "test/#", 3, "cid", true, 10_000, 30, 10,
                null, null);

        Set<ConstraintViolation<MqttProperties>> violations = validator.validate(props);

        assertFalse(violations.isEmpty());
    }

    @Test
    public void rejectsNonPositiveMaxInflight() {
        MqttProperties props = new MqttProperties("tcp://localhost:1883", "test/#", 1, "cid", true, 0, 30, 10, null,
                null);

        Set<ConstraintViolation<MqttProperties>> violations = validator.validate(props);

        assertFalse(violations.isEmpty());
    }

    @Test
    public void rejectsNonPositiveKeepAlive() {
        MqttProperties props = new MqttProperties("tcp://localhost:1883", "test/#", 1, "cid", true, 10_000, 0, 10, null,
                null);

        Set<ConstraintViolation<MqttProperties>> violations = validator.validate(props);

        assertFalse(violations.isEmpty());
    }

    @Test
    public void rejectsNonPositiveConnectionTimeout() {
        MqttProperties props = new MqttProperties("tcp://localhost:1883", "test/#", 1, "cid", true, 10_000, 30, -1,
                null, null);

        Set<ConstraintViolation<MqttProperties>> violations = validator.validate(props);

        assertFalse(violations.isEmpty());
    }
}
