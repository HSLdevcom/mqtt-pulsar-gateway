package fi.hsl.pulsar.mqtt.models;

public class MetroScheduleRouteRow {
    public long routerowId;
    public String station;
    public String platform;
    public String arrivalTimePlanned;
    public String arrivalTimeForecast;
    public String arrivalTimeMeasured;
    public String departureTimePlanned;
    public String departureTimeForecast;
    public String departureTimeMeasured;
    public String source;
    public MetroProgress rowProgress;
}
