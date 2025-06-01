package Météorologiques;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Locale;
import java.util.Properties;

public class WeatherStreamProcessing {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        // Étape 1 : Lire les messages depuis le topic "weather-data"
        KStream<String, String> weatherStream = builder.stream("weather-data");

        // Étapes 2 à 4 : Filtrer les températures > 30°C et convertir en °F
        KStream<String, String> filteredStream = weatherStream
                .mapValues(value -> value.trim())
                .filter((key, value) -> {
                    try {
                        String[] parts = value.split(",");
                        return parts.length == 3 && Double.parseDouble(parts[1]) > 30.0;
                    } catch (Exception e) {
                        return false;
                    }
                })
                .map((key, value) -> {
                    String[] parts = value.split(",");
                    String station = parts[0];
                    double tempC = Double.parseDouble(parts[1]);
                    double humidity = Double.parseDouble(parts[2]);

                    double tempF = (tempC * 9 / 5) + 32;
                    String newValue = String.format(Locale.US, "%.2f,%.2f", tempF, humidity);

                    return KeyValue.pair(station, newValue);
                });

        // Grouper par station
        KGroupedStream<String, String> groupedByStation = filteredStream.groupByKey();

        // Agréger pour calculer les moyennes
        KTable<String, String> stationAverages = groupedByStation.aggregate(
                () -> "0.0,0.0,0",
                (station, newValue, aggregate) -> {
                    String[] newParts = newValue.split(",");
                    String[] aggParts = aggregate.split(",");

                    double newTemp = Double.parseDouble(newParts[0]);
                    double newHum = Double.parseDouble(newParts[1]);

                    double aggTemp = Double.parseDouble(aggParts[0]);
                    double aggHum = Double.parseDouble(aggParts[1]);
                    long count = Long.parseLong(aggParts[2]);

                    double totalTemp = aggTemp + newTemp;
                    double totalHum = aggHum + newHum;
                    long totalCount = count + 1;

                    return String.format(Locale.US, "%.2f,%.2f,%d", totalTemp, totalHum, totalCount);
                },
                Materialized.with(Serdes.String(), Serdes.String())
        ).mapValues(agg -> {
            String[] parts = agg.split(",");
            double sumTemp = Double.parseDouble(parts[0]);
            double sumHum = Double.parseDouble(parts[1]);
            long count = Long.parseLong(parts[2]);

            double avgTemp = sumTemp / count;
            double avgHum = sumHum / count;

            return String.format(Locale.US, "Température Moyenne = %.2f°F, Humidité Moyenne = %.2f%%", avgTemp, avgHum);
        });

        // Écriture dans le topic "station-averages"
        stationAverages
                .toStream()
                .map((station, value) -> KeyValue.pair(station, station + " : " + value))
                .to("station-averages", Produced.with(Serdes.String(), Serdes.String()));

        // Lancer l'application Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        // Fermeture propre
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // Empêche l'arrêt du programme
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        //
         // kafka-console-producer --bootstrap-server localhost:9092 --topic weather-data
         //kafka-console-consumer --bootstrap-server localhost:9092 --topic station-averages --from-beginning
        //
    }
}
