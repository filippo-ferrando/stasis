RED    := \033[0;31m
GREEN  := \033[0;32m
YELLOW := \033[0;33m
BLUE   := \033[0;34m
NC     := \033[0m # No Color

CC := docker

DOCKER_FOLDER := ./docker
CHAIN := stasis-chain
WATCHDOG := stasis-watchdog
CHAIN_DF := blockchain.Dockerfile
WATCHDOG_DF := watchdog.Dockerfile
COMPOSE := docker-compose.yml

.PHONY: clean build run full test

clean:
	@printf "$(YELLOW)--- Stopping and removing containers... ---$(NC)\n"
	-$(CC) stop $$($(CC) ps -q --filter "name=$(CHAIN)") 2>/dev/null || true
	-$(CC) stop $$($(CC) ps -q --filter "name=$(WATCHDOG)") 2>/dev/null || true
	-$(CC) rm $$($(CC) ps -aq --filter "name=$(CHAIN)") 2>/dev/null || true
	-$(CC) rm $$($(CC) ps -aq --filter "name=$(WATCHDOG)") 2>/dev/null || true
	sudo rm -rf $(DOCKER_FOLDER)/test/
	@printf "\n$(RED)--- Removing images... ---$(NC)\n"
	-$(CC) rmi $(CHAIN):latest 2>/dev/null || true
	-$(CC) rmi $(WATCHDOG):latest 2>/dev/null || true

build:
	@printf "$(BLUE)--- Building blockchain image... ---$(NC)\n"
	$(CC) build -t $(CHAIN) -f $(DOCKER_FOLDER)/$(CHAIN_DF) .
	@printf "\n$(BLUE)--- Building watchdog image... ---$(NC)\n"
	$(CC) build -t $(WATCHDOG) -f $(DOCKER_FOLDER)/$(WATCHDOG_DF) .

run:
	@printf "$(GREEN)--- Starting local compose env... ---$(NC)\n"
	$(CC) compose -f $(DOCKER_FOLDER)/$(COMPOSE) up -d

full: clean 
	@printf "\n"
	${MAKE} build

test: clean
	@printf "\n"
	$(MAKE) build
	@printf "\n"
	$(MAKE) run
	@printf "\n$(GREEN)--- Test deployment complete! ---$(NC)\n"
