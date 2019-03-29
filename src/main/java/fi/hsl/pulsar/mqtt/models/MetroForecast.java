package fi.hsl.pulsar.mqtt.models;

import java.util.List;

public class MetroForecast {
    public List<Train> trains;
}

enum TrainState {
    PLANNED, APPROACHING, ARRIVING, ARRIVED, DEPARTING, DEPARTED
}

class Train {
    public String shiftNumber;
    public String originStation;
    public String destinationStation;
    public String jsId;
    public String arrivalTimeForecast;
    public String departureTimeForecast;
    public TrainState trainState;
    public TrainType trainType;
}

