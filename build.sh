#!/bin/sh

COMMIT_HASH=$(git rev-parse --short HEAD)

# Tag is composed with burrow version and last commit.
BUILD_TAG=${BUILD_TAG:-"1.0.0-$COMMIT_HASH"}

BUILD_DIR=$(dirname $0)

# Build the latest & specific tag version image.
docker build -t go-rainbow:latest  \
             -t go-rainbow:$BUILD_TAG \
             $BUILD_DIR
