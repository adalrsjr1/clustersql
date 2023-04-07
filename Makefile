# Docker variables
DOCKER_IMAGE_NAME := adalrsjr1/clustersql
DOCKER_IMAGE_TAG := latest

.PHONY: all build

all: build

build:
	@echo "Building Docker image..."
	@docker build -t $(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_TAG) -f build/Dockerfile .