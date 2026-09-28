#!/bin/sh

profile=local
region=eu-west-1

#Queues
declare -a queues=(
		${profile}-test-queue
	)

#Topics
declare -a topics=(
		TestTopic
	)

#Buckets
declare -a buckets=(
		test-bucket-${profile}
	)

for queue in "${queues[@]}"
do
	aws --endpoint-url=http://localhost:4566 sqs create-queue --attributes VisibilityTimeout=300 --queue-name $queue
done


for topic in "${topics[@]}"
do
	aws --endpoint-url=http://localhost:4566 sns create-topic --name $topic
done

for bucket in "${buckets[@]}"
do
	aws --endpoint-url=http://localhost:4566 s3 mb s3://$bucket
done
