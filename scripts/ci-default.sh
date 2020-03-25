#!/bin/sh
# Login docker
echo "${DOCKER_PASSWORD}" | docker login -u "${DOCKER_USERNAME}" --password-stdin
# Build Golang Application
go build -o dist/elastic-queue-logger
# Build docker image
docker build . -t kainonly/elastic-queue-logger:latest
# Push docker image
docker push kainonly/elastic-queue-logger:latest