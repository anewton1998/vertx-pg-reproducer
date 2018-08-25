About
=====
This project reproduces a bug with Vert.X and/or the Reactiverse PostgreSQL driver whereby an `IllegalStateException`
is thrown by the `io.reactiverse.pgclient.impl.SocketConnection.schedule(SocketConnection.java:173)` because it
believes it's Vertx `Context` has changed.

This code transfers data between two PostgreSQL databases using a verticle. Two verticle implementations are given:
one using the standard API and one using the ReactiveX API. Both veritcles produce the same exception.

This code is written in Kotlin.

Prerequisites
=============

To reproduce this, the following are needed:

* Java 8
* Docker
* Docker-compose

This project uses Gradle, but the Gradle Wrapper is included.

Setup
=====

Create two databases with docker-compose:

```
docker-compose up
```

Initialize the source database with data:

```
./gradlew srcDbInit
```

You can log into the databases to check thing out using

```
docker exec -it vertxpgreproducer_srcpg_1 psql -U postgres
```

and

```
docker exec -it vertxpgreproducer_destpg_1 psql -U postgres
```

where `vertxpgreproducer_srcpg_1` and `vertxpgreproducer_destpg_1` are the docker container names. You may need to issue
`docker ps -a` to if those aren't the names created for you by docker-compose.

Running
=======

The run and re-run the code, here are the following steps.

To initialize the destination database and set it up:

```
./gradlew flywayClean flywayMigrate
```

To compile the code and create a fat-jar:

```
./gradlew jar
```

To run the code:

```
java -jar build/libs/vertx-pg-reproducer.jar -conf src/test/resources/config.json
```

The `config.json` file controls a couple of settings for altering the test. The two most interesting are
`fetch_size` and `test_type`. `fetch_size` controls the size of the PgStream requested. And `test_type` switches
the verticle implementation from the standard verticle to the ReactiveX verticle.