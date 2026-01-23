package fi.hsl.pulsar.mqtt;

import fi.hsl.common.health.HealthServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.context.SmartLifecycle;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class HealthServerRegistrar implements SmartLifecycle {

    private final static Logger logger = LoggerFactory.getLogger(HealthServerRegistrar.class);

    private final HealthServer healthServer;
    private final MessageProcessor messageProcessor;
    private final MqttPahoMessageDrivenChannelAdapter mqttAdapter;
    private final List<HealthIndicator> healthIndicators;

    private volatile boolean running = false;

    public HealthServerRegistrar(HealthServer healthServer, IMqttMessageHandler mqttMessageHandler,
            MqttPahoMessageDrivenChannelAdapter mqttAdapter, List<HealthIndicator> healthIndicators) {
        this.healthServer = healthServer;
        this.messageProcessor = (MessageProcessor) mqttMessageHandler;
        this.mqttAdapter = mqttAdapter;
        this.healthIndicators = healthIndicators;
    }

    @Override
    public void start() {
        if (healthServer == null) {
            logger.info("HealthServer is null; skipping health check registration.");
            running = false;
        }

        healthServer.addCheck(() -> {
            boolean mqttRunning = mqttAdapter.isRunning();
            if (!mqttRunning) {
                logger.error("MQTT is not running.");
            }
            return mqttRunning;
        });
        healthServer.addCheck(messageProcessor::isLastMsgSendIntervalHealthy);

        for (HealthIndicator healthIndicator : healthIndicators) {
            healthServer.addCheck(() -> {
                Health health = healthIndicator.health();
                return health != null && "UP".equalsIgnoreCase(health.getStatus().getCode());
            });
        }

        running = true;
    }

    @Override
    public void stop() {
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }
}
