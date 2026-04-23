package fi.hsl.pulsar.mqtt.config;

import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based tests for {@link MqttProperties#credentials()} and
 * {@link MqttProperties#isCredentialsComplete()}.
 *
 * <p>These complement the example-based tests in {@link MqttPropertiesTest}
 * by exploring the full input space of username and password combinations
 * including null, empty, blank (whitespace-only), and non-blank strings.</p>
 */
class MqttPropertiesPropertyTest {

    private static MqttProperties propsWithCredentials(String username, String password) {
        return new MqttProperties("tcp://localhost:1883", "test/#", 1, "cid", true, 10_000, 30, 10, username, password);
    }

    private static boolean hasValue(String value) {
        return value != null && !value.isBlank();
    }

    /**
     * {@code credentials()} returns a present {@link Optional} if and only if
     * both username and password are non-null and non-blank.
     */
    @Property
    void credentialsPresentIffBothHaveValue(@ForAll("credentialStrings") String username,
            @ForAll("credentialStrings") String password) {
        MqttProperties props = propsWithCredentials(username, password);
        boolean expectedPresent = hasValue(username) && hasValue(password);

        assertEquals(expectedPresent, props.credentials().isPresent());
    }

    /**
     * When {@code credentials()} returns a present value, it wraps the
     * original username and password unchanged.
     */
    @Property
    void credentialsWrapsOriginalValues(@ForAll("nonBlankStrings") String username,
            @ForAll("nonBlankStrings") String password) {
        MqttProperties props = propsWithCredentials(username, password);

        Optional<MqttProperties.Credentials> credentials = props.credentials();
        assertTrue(credentials.isPresent());
        assertEquals(username, credentials.get().username());
        assertEquals(password, credentials.get().password());
    }

    /**
     * {@code isCredentialsComplete()} returns true if and only if the
     * "has-value" status of username equals the "has-value" status of
     * password, i.e. both present or both absent.
     */
    @Property
    void credentialsCompleteIffSymmetric(@ForAll("credentialStrings") String username,
            @ForAll("credentialStrings") String password) {
        MqttProperties props = propsWithCredentials(username, password);

        assertEquals(hasValue(username) == hasValue(password), props.isCredentialsComplete());
    }

    /**
     * Whenever {@code credentials()} returns a present value,
     * {@code isCredentialsComplete()} must also return true. The two
     * methods must stay consistent with each other.
     */
    @Property
    void credentialsPresentImpliesComplete(@ForAll("credentialStrings") String username,
            @ForAll("credentialStrings") String password) {
        MqttProperties props = propsWithCredentials(username, password);

        if (props.credentials().isPresent()) {
            assertTrue(props.isCredentialsComplete());
        }
    }

    /**
     * Generates strings covering the four credential categories: null,
     * empty, blank (whitespace-only), and arbitrary non-blank strings.
     */
    @Provide
    Arbitrary<String> credentialStrings() {
        Arbitrary<String> nullValue = net.jqwik.api.Arbitraries.just(null);
        Arbitrary<String> empty = net.jqwik.api.Arbitraries.just("");
        Arbitrary<String> blank = net.jqwik.api.Arbitraries.of(" ", "\t", "\n", "  \t\n  ");
        Arbitrary<String> nonBlank = net.jqwik.api.Arbitraries.strings().ofMinLength(1).filter(s -> !s.isBlank());
        return net.jqwik.api.Arbitraries.oneOf(nullValue, empty, blank, nonBlank);
    }

    /**
     * Generates non-blank strings for tests that require both credentials
     * to have a value.
     */
    @Provide
    Arbitrary<String> nonBlankStrings() {
        return net.jqwik.api.Arbitraries.strings().ofMinLength(1).filter(s -> !s.isBlank());
    }
}
