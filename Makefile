IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
PLATFORM ?= linux/amd64
DEV   ?= dev
ANSWER ?= dev-answer
export IMAGE CPUS MEM PLATFORM

CXX ?= g++
CXXFLAGS ?= -std=c++17 -O2 -fno-tree-vectorize -march=cascadelake -mtune=cascadelake -pthread -Wa,-mbranches-within-32B-boundaries

.PHONY: build native fixtures fixtures-native dev run-dev check-dev selftest fuzz

## Docker image for the scorer's platform
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

## Native binary (no Docker), for quick local loops
native: bin/shopping-sessions
bin/shopping-sessions: src/main.cpp src/dense.h src/parquet.h src/scan.h
	mkdir -p bin && $(CXX) $(CXXFLAGS) -o $@ src/main.cpp

## All 36 edge-case fixtures through the Docker image, with scorer flags
fixtures: build
	python3 scripts/run_fixtures.py fixtures -- scripts/docker_run.sh

## The same fixtures with the native binary
fixtures-native: native
	python3 scripts/run_fixtures.py fixtures -- bin/shopping-sessions

## The public 1M-event dev set (needs `generate` from generator.zip on PATH)
dev:
	generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

check-dev:
	python3 scripts/compare.py out $(ANSWER)

## SIMD timestamp parsers against scalar references (20M random cases)
selftest: native
	bin/shopping-sessions --selftest

## CSV scanner against a byte-by-byte reference
fuzz:
	mkdir -p bin && $(CXX) $(CXXFLAGS) -o bin/fuzz_scan tests/fuzz_scan.cpp && bin/fuzz_scan 20000
