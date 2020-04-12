# Spark Streaming Infection Chain

**TL;DR**: This service listens on a Kafka topic for events containing user contacts and 

## Usage

First, make sure that there's a Kafka instance running locally, this can be achieved by running:

```bash
docker-compose up
```

Then, start the `src/main/scala/Main` class. This project uses Java 1.8, newer versions will not be compatible with the used Spark version.

To test it out, we need to produce some messages:

```
kafka-console-producer --topic contacts --broker-list localhost:9092
```

At this point you can send some demo data through the producer shell, e.g. this snippet (submit with shift-enter as to prevent also sending an empty message):

```csv
bar,gas,1
hotel,restaurant,1
```

Besides emitting connections, we also need to mark users as infected. To emit infection events, open up a shell with the following command
```bash
kafka-console-producer --topic infections --broker-list localhost:9092
```

Here you can send user IDs to mark users as infected. The value before the comma is the user ID, the value after the comma the timestamp at which they got infected.
```csv
bar,0
```

We can open a consumer shell to see the `at_risk` messages arrive

```bash
kafka-console-consumer --bootstrap-server localhost:9092 --topic at_risk
```

Which will give messages formatted as `$userId,$level,$timestamp`, where `$level` is the amount of steps between someone who's infected and the current user.

```csv
bar,0,0
gas,1,1586705689509
```

## Test
To run the tests, run:
```bash
sbt test
```