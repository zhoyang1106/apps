#!/bin/bash

IMAGE_NAME="soar009/node-service-image"
TAG="v5"
DOCKERFILE_PATH="$HOME/apps/docker/apps/Dockerfile-v5"
CONTEXT_PATH="$HOME/apps/docker/apps"

echo "Build docker image"
sudo docker build -t $IMAGE_NAME:$TAG -f $DOCKERFILE_PATH $CONTEXT_PATH

if [ $? -ne 0 ]; then
    echo "Docker image build failed!"
    exit 1
fi

echo "Push Image to Docker Hub..."
sudo docker push $IMAGE_NAME:$TAG


if [ $? -ne 0 ]; then
    echo "Docker image push failed!"
    exit 1
fi

echo "Docker image push finish:  $IMAGE_NAME:$TAG"
