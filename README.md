# Spark Streaming Infection Chain

**TL;DR**: This service listens on a Kafka topic for events containing user contacts and 

## Usage

First, make sure that there's a Kafka instance running locally, this can be achieved by running:

```bash
docker-compose up
```

Then, start the `src/main/scala/TestComputeAtRisk` class. This project uses Java 1.8, newer versions will not be compatible with the used Spark version.

To play around with it, open two shells, one running:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic result
```

And another one running:

```
kafka-console-producer --topic contacts --broker-list localhost:9092
```

At this point you can send some demo data through the producer shell, e.g. this snippet (submit with shift-enter as to prevent also sending an empty message):

```csv
bar,gas,1
hotel,restaurant,1
```

To emit infection events, open up a shell with the following command
```bash
kafka-console-producer --topic infections --broker-list localhost:9092
```

Here you can send user IDs to mark users as infected.
```csv
bar
```
