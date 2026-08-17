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

## Elasticsearch session indices

Session documents are written to one index per day, `radius-sessions-yyyy.MM.dd`,
named in the deployment timezone (`TZ`) — the same zone the timestamps on the
document are converted to.

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
