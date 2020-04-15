# kafka_offset_migrator

It's well known that Kafka consumer group offset history or state is permanent as long as consumers in said group remain online. This becomes a problem when a particular group's topic subscription changes over time and older topics are stuck in the offset tracking. In which case, the consumer lag will build up for this topic and can really hinder getting sensible consumer group lag metrics.

This simple script was created to handle such a situation. Additionally, it can help in taking a large consumer group containing consumers subscribing to various topics and splitting them up into multiple consumer groups, and you need to keep their offset state intact. Especially important if you are dealing with consumers that are _not_ idempotent.

## Dependencies

This depends on Python 3 and the [kafka-python](https://pypi.org/project/kafka-python/) package.

## Usage

Simply run and follow the prompts.

