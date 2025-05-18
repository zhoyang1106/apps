#!/bin/bash

# 设置变量
IMAGE_NAME="soar009/node-service-image"  # 替换为你的Docker Hub用户名和存储库名
TAG="v6"  # 你可以根据需要设置镜像标签
DOCKERFILE_PATH="$HOME/apps/docker/apps/Dockerfile-v6"  # Dockerfile 文件名
CONTEXT_PATH="$HOME/apps/docker/apps"

# 1. 构建 Docker 镜像
echo "构建 Docker 镜像..."
sudo docker build -t $IMAGE_NAME:$TAG -f $DOCKERFILE_PATH $CONTEXT_PATH

# 检查构建是否成功
if [ $? -ne 0 ]; then
    echo "Docker 镜像构建失败！"
    exit 1
fi

# 2. 推送镜像到 Docker Hub
echo "推送镜像到 Docker Hub..."
sudo docker push $IMAGE_NAME:$TAG

# 检查推送是否成功
if [ $? -ne 0 ]; then
    echo "Docker 镜像推送失败！"
    exit 1
fi

echo "Docker 镜像成功推送到 $IMAGE_NAME:$TAG"
