JAR := flink-jobs/target/flink-feature-platform-jobs-0.1.0.jar
MONTH ?= 2024-01
SPEEDUP ?= 60
ARGS ?=
REPLAY_ARGS := --bootstrap=kafka:19092 --schema-registry=http://schema-registry:8081 --schema=/app/schemas/trip_event.avsc

# Uses your local Maven if there is one, otherwise Maven in Docker (needs nothing but Docker).
ifeq ($(shell command -v mvn 2>/dev/null),)
MVN := docker run --rm -v "$(CURDIR)":/w -v "$(HOME)/.m2":/root/.m2 -w /w/flink-jobs maven:3.9-eclipse-temurin-17 mvn
else
MVN := mvn -f flink-jobs/pom.xml
endif

.PHONY: help build test up down submit submit-smoke replay replay-tlc data logs late rows corrections topic-count clean

help: ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F ':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

build: ## compile, test, and package the Flink job jar
	$(MVN) -B verify

test: ## Java unit tests and replayer tests
	$(MVN) -B test
	cd replayer && python3 -m pytest -q

up: ## start Kafka, Schema Registry, and Flink
	docker compose up -d --wait kafka schema-registry jobmanager taskmanager
	docker compose up kafka-init

down: ## stop everything and delete Kafka data
	docker compose --profile replay down -v

submit: $(JAR) ## submit the feature job; pass job flags with ARGS="--allowed-lateness-s 60"
	docker compose exec jobmanager flink run -d /opt/flink/usrlib/$(notdir $(JAR)) $(ARGS)

submit-smoke: $(JAR) ## submit the week 1 trip-count job instead
	docker compose exec jobmanager flink run -d -c io.github.sahilpatkar.ffp.TripCountJob /opt/flink/usrlib/$(notdir $(JAR)) $(ARGS)

$(JAR):
	$(MAKE) build

replay: ## replay 50k synthetic trips (no download needed)
	docker compose run --rm --build replayer $(REPLAY_ARGS) --synthetic=50000 --speedup=$(SPEEDUP)

data: ## download one month of NYC TLC trips into data/
	bash scripts/download_data.sh $(MONTH)

replay-tlc: data ## replay real TLC trips
	docker compose run --rm --build replayer $(REPLAY_ARGS) --file=/data/yellow_tripdata_$(MONTH).parquet --speedup=$(SPEEDUP)

logs: ## follow the job's output (feature rows and late trips)
	docker compose logs -f taskmanager | grep --line-buffered -E '(feature|count|late)(:[0-9]+)?> '

late: ## trips that arrived too late to be counted
	@docker compose logs taskmanager | grep -cE 'late(:[0-9]+)?> ' || true

rows: ## feature rows emitted so far
	@docker compose logs taskmanager | grep -cE 'feature(:[0-9]+)?> ' || true

corrections: ## feature rows re-emitted because late data changed them
	@docker compose logs taskmanager | grep -E 'feature(:[0-9]+)?> ' | grep -c 'correction=true' || true

topic-count: ## number of messages in trips.raw
	@docker compose exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:19092 --topic trips.raw \
	  | awk -F: '{s+=$$3} END {print s}'

clean:
	$(MVN) -B clean
