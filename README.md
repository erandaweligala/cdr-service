# Quarkus sample Template

This project uses Quarkus, the Supersonic Subatomic Java Framework.

If you want to learn more about Quarkus, please visit its website: <https://quarkus.io/>.

## Running the application in dev mode

You can run your application in dev mode that enables live coding using:

```shell script
./mvnw quarkus:dev
```

> **_NOTE:_**  Quarkus now ships with a Dev UI, which is available in dev mode only at <http://localhost:8080/q/dev/>.

## Packaging and running the application

The application can be packaged using:

```shell script
./mvnw package
```

It produces the `quarkus-run.jar` file in the `target/quarkus-app/` directory.
Be aware that it’s not an _über-jar_ as the dependencies are copied into the `target/quarkus-app/lib/` directory.

The application is now runnable using `java -jar target/quarkus-app/quarkus-run.jar`.

If you want to build an _über-jar_, execute the following command:

```shell script
./mvnw package -Dquarkus.package.jar.type=uber-jar
```

The application, packaged as an _über-jar_, is now runnable using `java -jar target/*-runner.jar`.

## Creating a native executable

You can create a native executable using:

```shell script
./mvnw package -Dnative
```

Or, if you don't have GraalVM installed, you can run the native executable build in a container using:

```shell script
./mvnw package -Dnative -Dquarkus.native.container-build=true
```

You can then execute your native executable with: `./target/radius-client-1.0.0-SNAPSHOT-runner`

If you want to learn more about building native executables, please consult <https://quarkus.io/guides/maven-tooling>.

## Related Guides

- REST ([guide](https://quarkus.io/guides/rest)): A Jakarta REST implementation utilizing build time processing and Vert.x. This extension is not compatible with the quarkus-resteasy extension, or any of the extensions that depend on it.
- REST Jackson ([guide](https://quarkus.io/guides/rest#json-serialisation)): Jackson serialization support for Quarkus REST. This extension is not compatible with the quarkus-resteasy extension, or any of the extensions that depend on it
- Hibernate ORM with Panache ([guide](https://quarkus.io/guides/hibernate-orm-panache)): Simplify your persistence code for Hibernate ORM via the active record or the repository pattern
- JDBC Driver - PostgreSQL ([guide](https://quarkus.io/guides/datasource)): Connect to the PostgreSQL database via JDBC

## Provided Code

### Hibernate ORM

Create your first JPA entity

[Related guide section...](https://quarkus.io/guides/hibernate-orm)

[Related Hibernate with Panache section...](https://quarkus.io/guides/hibernate-orm-panache)


### REST

Easily start your REST Web Services

[Related guide section...](https://quarkus.io/guides/getting-started-reactive#reactive-jax-rs-resources)

## Timestamps and the deployment time zone

Every timestamp the service records is an absolute instant: the `startTime`,
`endTime` and `updatedTime` of a session, and the `dateTime` of each of its
instances. An instant carries no time zone of its own, so each place that turns
one back into a date a person reads has to pick a zone — and they all have to
pick the same one. That zone is `app.timezone`, which defaults to the pod's `TZ`
environment variable:

```yaml
app:
  timezone: "${TZ:UTC}"
```

The API converts to it on the way out and serializes an ISO-8601 local
date-time with no offset (`2026-08-17T20:05:45.967`). Nothing downstream
re-converts an offset-less timestamp, so the session list and the session
instance details show the same clock time for the same event. Previously both
were serialized in UTC (`2026-08-17T17:05:45.967+00:00`) and the admin console
converted the offset on the list but printed it verbatim on the details, so one
session read as starting at 20:05 with its own events logged at 17:05.

The same zone reads the `startTime`/`endTime` bounds of a session search, which
arrive the way they are displayed — offset-less wall-clock times (a plain
`yyyy-MM-dd` is accepted too, and an explicit offset is honoured as given).

## Forwarding events to the Airtel topic

Every `AccountingEvent` the service consumes **from its own zone's CDR topic**
is republished, unchanged, to the Airtel Kafka topic. The format is the consumed
one: the same `AccountingEvent` object is handed to the outgoing channel, which
serializes it with the same application `ObjectMapper` the incoming channel
deserialized it with, so no field is added, dropped or renamed. The record key is the consumed record's own
key, so events keep their partition — and with it their per-session order —
falling back to the event's `partitionKey` and then its `eventId` when the
consumed record carries no key.

Within that channel the forward is unconditional. It is started before the event
is routed and runs independently of it, so an event reaches the topic whatever
its `eventType` (including one the router does not handle, or none at all) and
whatever happens downstream — a Redis or Elasticsearch outage, a malformed
payload, any exception at all. It also runs concurrently with processing, so the
round trip to the Airtel broker does not add to the per-event latency.

### Mirrored CDRs are not forwarded

The service consumes two CDR streams: `accounting-cdr-events`, this zone's own
topic, and `accounting-cdr-events-mirror`, the peer zone's topic replicated here
by MirrorMaker. Both feed Elasticsearch — that cross-mirroring is what makes the
session data in DC and in DR complete, and it is unchanged.

Only the first is forwarded to Airtel. DC and DR both run this service against
the same Airtel topic, so a CDR produced in DR is published by the DR service
off its primary channel and, when the mirror channel forwarded too, published a
second time by the DC service off `dr.cdr-event-dr` — the same CDR on the Airtel
topic twice. Publishing only from the primary channel makes the zone a CDR was
produced in the one zone that publishes it, so each CDR reaches Airtel exactly
once, from whichever zone produced it, with no change to the Elasticsearch flow.

The exception is a failover: if the peer zone's cdr-service is down while
MirrorMaker still delivers its CDRs, nothing is publishing them. Setting
`publish-from-mirror` to `true` in the surviving zone has it publish those too
until the peer is back — and it has to be set back to `false` then, or CDRs are
published twice again.

The traffic is one way: a failure to publish is logged and counted as a
`producer`/`kafka` exception on the error dashboards, but it never fails an
event or stalls a consumer, and the Airtel broker is deliberately left out of
the service's health checks so an outage there cannot take the pod out of
service. A broker that stops answering altogether is waited on for
`publish-timeout-ms` at most — past that the consumer moves on while the record
stays queued in the producer, which still delivers it once the broker returns.

The cluster, the topic, that wait and the failover override are configurable, and
the cluster defaults to the one the events are consumed from:

```yaml
airtel:
  kafka:
    bootstrap-servers: "${AIRTEL_KAFKA_BOOTSTRAP_SERVERS:kafka-headless.cluster-dc.svc.cluster.local:9092}"
    topic: "${AIRTEL_KAFKA_TOPIC:cdr-event-airtel}"
    publish-timeout-ms: "${AIRTEL_KAFKA_PUBLISH_TIMEOUT_MS:10000}"
    publish-from-mirror: "${AIRTEL_KAFKA_PUBLISH_FROM_MIRROR:false}"
```

One thing to check on the broker side: the Airtel topic itself must stay out of
the MirrorMaker topic list. It carries the CDRs of both zones already, so
replicating it to the peer cluster only creates a second copy for anything that
consumes the Airtel stream from both clusters.

## Elasticsearch session indices

Session documents are written to one index per day, `radius-sessions-yyyy.MM.dd`,
named in the same zone, so a session belongs to the index for the date the
console shows it under.

The mapping of those indices is owned by the application: on startup it installs
the composable index template `radius-sessions-template` (pattern
`radius-sessions-*`), so each daily index is created with `startTime`, `endTime`,
`updatedTime` and `sessionInstances.dateTime` mapped as `date`, and with the
`.keyword` sub-fields the search queries filter on. Without the template the
mapping is inferred from whichever document happens to be indexed first, which is
how these fields ended up typed as `long` and produced

```
document_parsing_exception ... failed to parse field [startTime] of type [long]
query_shard_exception: failed to create query: For input string: "2026-08-16T00:00:00"
```

Set `elasticsearch.index-template.enabled=false` if the template is managed
outside the application.

### Repairing an index that predates the template

A template is applied only when an index is created, so an existing index keeps
its old mapping — and the type of a field that already exists cannot be changed
in place. Startup logs an error naming any such index. To repair one, reindex it
into a new index created from the template:

```shell script
# 1. create the fixed index from the template
curl -X PUT "$ES/radius-sessions-2026.08.17-fixed"

# 2. copy the documents, converting the epoch values to the date mapping
curl -X POST "$ES/_reindex" -H 'Content-Type: application/json' -d '{
  "source": { "index": "radius-sessions-2026.08.17" },
  "dest":   { "index": "radius-sessions-2026.08.17-fixed" }
}'

# 3. swap the name over once the counts match
curl -X DELETE "$ES/radius-sessions-2026.08.17"
curl -X POST "$ES/_aliases" -H 'Content-Type: application/json' -d '{
  "actions": [{ "add": { "index": "radius-sessions-2026.08.17-fixed",
                         "alias": "radius-sessions-2026.08.17" }}]
}'
```
