RED    := \033[0;31m
GREEN  := \033[0;32m
YELLOW := \033[0;33m
BLUE   := \033[0;34m
NC     := \033[0m # No Color

CC := docker

DOCKER_FOLDER := ./docker
CHAIN := stasis-chain
WATCHDOG := stasis-watchdog
CHAIN_DF := stasis-api.Dockerfile
WATCHDOG_DF := stasis-watchdog.Dockerfile
COMPOSE := docker-compose.yml

BUILDER := stasis-builder

arch ?= x64

ifeq ($(arch),x64)
	PLATFORM := linux/amd64
else ifeq ($(arch),arm64)
	PLATFORM := linux/arm64
else ifeq ($(arch),multi)
	PLATFORM := linux/amd64,linux/arm64
else
$(error Unsupported arch '$(arch)'.)
endif

.PHONY: clean build run full test builder

builder:
	@printf "$(YELLOW)--- Ensuring buildx builder exists... ---$(NC)\n"
	-$(CC) buildx create --name $(BUILDER) --use 2>/dev/null || true
	@$(CC) buildx inspect --bootstrap > /dev/null

clean:
	@printf "$(YELLOW)--- Stopping and removing containers... ---$(NC)\n"
	-$(CC) compose -f $(DOCKER_FOLDER)/$(COMPOSE) down -v --rmi all 2>/dev/null || true

	@printf "$(YELLOW)--- Removing buildx builder ($(BUILDER))... ---$(NC)\n"
	-$(CC) buildx rm $(BUILDER) 2>/dev/null || true

	@printf "$(YELLOW)--- Pruning buildx cache... ---$(NC)\n"
	-$(CC) buildx prune -af 2>/dev/null || true

	@printf "$(YELLOW)--- Cleaning local test artifacts... ---$(NC)\n"
	sudo rm -rf $(DOCKER_FOLDER)/test/

build: builder
	@printf "$(BLUE)--- Building stasis image ($(PLATFORM))... ---$(NC)\n"
	$(CC) buildx build \
		--platform=$(PLATFORM) \
		--load \
		-t $(CHAIN) \
		-f $(DOCKER_FOLDER)/$(CHAIN_DF) .

	@printf "\n$(BLUE)--- Building watchdog image ($(PLATFORM))... ---$(NC)\n"
	$(CC) buildx build \
		--platform=$(PLATFORM) \
		--load \
		-t $(WATCHDOG) \
		-f $(DOCKER_FOLDER)/$(WATCHDOG_DF) .

run:
	@printf "$(GREEN)--- Starting local compose env... ---$(NC)\n"
	$(CC) compose -f $(DOCKER_FOLDER)/$(COMPOSE) up -d

full: clean
	@printf "\n"
	$(MAKE) build arch=$(arch)

test: clean build run
	@printf "\n$(GREEN)--- Test deployment complete! ---$(NC)\n"
