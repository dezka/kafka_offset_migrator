#!/usr/bin/python
''' 
Description:
 * This program is designed to migrate consumer group offset state to a new consumer group.
 An example of this would be when you're taking a large consumer group, and splitting it up into smaller ones, however,
 you want these new consumer groups to leave off exactly where the last group stopped to prevent reprocessing or missing data.

Authors:
 * Dennis T. Bielinski
'''

from kafka import KafkaAdminClient
import os
import subprocess
import sys

# :`str`: Intermediate file used to hand off the offset information to the CLI tool. Deleted after processing.
OUTPUT_FILE = os.path.dirname(os.path.abspath(
    __file__)) + '/_generated_offset_reset.csv'


def collect_old_consumer_group_offsets(bootstrap_servers, old_consumer_group, removed_topics):
    """
    Connects to the brokers specified to gather current offset information of the consumer group we're migrating from.

    :param bootstrap_servers: The Kafka brokers in the cluster to connect to.
    :param old_consumer_group: The consumer group we are migrating from.
    """
    adminClient = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    results = adminClient.list_consumer_group_offsets(old_consumer_group)

    delimeter = ','
    with open(OUTPUT_FILE, 'w') as f:
        for k, v in results.items():
            if len(removed_topics) > 0:
                topic = k._asdict()['topic']
                if topic in removed_topics:
                    continue

            f.write(str(k._asdict()['topic']) + delimeter)
            f.write(str(k._asdict()['partition']) + delimeter)
            f.write(str(v._asdict()['offset']) + '\n')

    adminClient.close()


def collect_topic_information(bootstrap_servers, old_consumer_group):
    """Gets a list of current topics being subscribed to by this consumer group that we may need to remove with the migration.

    Using the `list_consumer_group_offsets()` function since `describe_consumer_groups()` doesn't return proper data.

    :param bootstrap_servers: The Kafka brokers in the cluster to connect to.
    :param old_consumer_group: The consumer group we are migrating from.
    """
    adminClient = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    results = adminClient.list_consumer_group_offsets(old_consumer_group)

    topics = []
    for k, v in results.items():
        topic = k._asdict()['topic']
        if topic not in topics:
            topics.append(topic)

    adminClient.close()
    return topics


def run_consumer_reset(bootstrap_servers, new_consumer_group):
    """Runs the consumer resetting process.

    :param bootstrap_servers: The Kafka brokers in the cluster to connect to.
    :param new_consumer_group: The new consumer group we want to migrate the offsets to.
    """
    dry_run_command = "/usr/bin/kafka-consumer-groups --bootstrap-server {0} --group {1} --reset-offsets --from-file {2} --dry-run".format(
        bootstrap_servers, new_consumer_group, OUTPUT_FILE)
    execute_command = "/usr/bin/kafka-consumer-groups --bootstrap-server {0} --group {1} --reset-offsets --from-file {2} --execute".format(
        bootstrap_servers, new_consumer_group, OUTPUT_FILE)

    subprocess.run(dry_run_command.split())

    while True:
        print("\n************************************************")
        response = input(
            'Do the proposed changes look good to you? (y/n): ')
        if not response.lower().strip()[:1] == 'y':
            os.remove(OUTPUT_FILE)
            sys.exit(1)
        else:
            print('\nProceeding...\n')
            response = input(
                'Are you sure you want to execute these offset changes? (y/n): ')
            if not response.lower().strip()[:1] == 'y':
                os.remove(OUTPUT_FILE)
                sys.exit(1)
            else:
                subprocess.run(execute_command.split())
                print('\nCleaning up...\n')
                os.remove(OUTPUT_FILE)
                print("DONE!")
                sys.exit(0)


if __name__ == '__main__':
    bootstrap_servers = input(
        '\nPlease enter the Kafka broker hostnames in a comma separated list (e.g. kafka1:9092,kafka2:9092,kafka3:9092): ')
    print("\nUsing brokers: {}".format(bootstrap_servers))

    old_consumer_group = input(
        '\nPlease enter the old consumer group you are migrating from: ')
    print("\nOld consumer group is set as: {}".format(old_consumer_group))

    old_group_topic_list = collect_topic_information(
        bootstrap_servers, old_consumer_group)
    print("\n{0} is curently subscribed to the following topics: {1}".format(
        old_consumer_group, ', '.join(old_group_topic_list)))

    removed_topics = []
    remove_topics = input(
        '\nWould you like to remove any of these topics so they are not part of the new consumer group? '
    )
    if remove_topics.lower().strip()[:1] == 'y':
        removed_topics = input(
            '\nPlease enter the topic(s) in the above list you want to remove (e.g. topic1, topic2, topic3): '
        ).replace(' ', '').split(',')

    print("\n**************************************************************************************************\n")
    print("Please note, ALL consumers in this group must be turned off, and the consumer group must be empty!")
    print("\n**************************************************************************************************\n")

    is_empty = input(
        'Have you confirmed all consumers are out of the group and it is empty? (y/n): ')
    if not is_empty.lower().strip()[:1] == 'y':
        sys.exit(1)

    print("\nGenerating offsets...")
    collect_old_consumer_group_offsets(
        bootstrap_servers, old_consumer_group, removed_topics)

    print("\nA CSV file containing current offsets for {0} has been generated at {1}, please verify this looks appropriate.".format(
        old_consumer_group, OUTPUT_FILE))
    proceed = input('\nProceed? (y/n): ')
    if not proceed.lower().strip()[:1] == 'y':
        os.remove(OUTPUT_FILE)
        sys.exit(1)

    new_consumer_group = input(
        '\nPlease enter the new consumer group you want to set these offsets to: ')
    print("\nNew consumer group is set as: {}\n".format(new_consumer_group))

    print("Generating an offset reset operation on {} ....\n".format(
        new_consumer_group))
    run_consumer_reset(bootstrap_servers, new_consumer_group)

