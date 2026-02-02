# Kafka producer plugin

This plugin allows you to produce messages to Kafka from HSL.

> **_Important!_**
>
> Kafka has a internal memory based queue. In case of a forceful application restart the queue may be lost. During graceful shutdown, the system will wait 60 seconds to drain queues.
> If you need guaranteed transaction safety it is recommended to write to a fsynced log file (eg. halon-extras-logger) and use eg. filebeat.

## Installation

Follow the [instructions](https://docs.halon.io/manual/comp_install.html#installation) in our manual to add our package repository and then run the below command.

### Ubuntu

```
apt-get install halon-extras-kafka
```

### RHEL

```
yum install halon-extras-kafka
```

## Commands

This plugin can be controlled using the ``halonctl`` tool. The following commands are available though ``halonctl plugin command kafka ...``.

| Command | |
|------|------|
| dump \<queue> | Show debug information |

Example 

```
halonctl plugin command kafka dump kafka1
```

## Configuration

For the configuration schema, see [kafka.schema.json](kafka.schema.json). Below is a sample configuration.

### smtpd.yaml

```
plugins:
- id: kafka
  config:
    queues:
    - id: kafka1
      config:
        bootstrap.servers: kafka:9092
        # queue.buffering.max.messages: 100000
```

## Exported functions

These functions needs to be [imported](https://docs.halon.io/hsl/structures.html#import) from the `extras://kafka` module path.

### kafka_producer(id, topic[, value[, key[, headers[, partition]]]])

**Params**

- id `string` (**required**) the id of the queue
- topic `string` (**required**) the topic
- value `string` value to be sent
- key `string` or `none` key to be sent
- headers `array` a list of headers to be sent
- partition `number` the partition to use, specificy -1 use the configured partitioner
- block `boolean` if the function should block when the internal message queue is full, the default is `false`

**Returns**

An array, currently only `errno` and `errstr` in case of errors. The most common error to handle would be queue full (error -184).

### Example

```
import { kafka_producer } from "extras://kafka";
echo kafka_producer("kafka1", "test-topic", "myvalue", "mykey", [
    "foo" => "bar",
    "biz" => "buz",
], -1);
```