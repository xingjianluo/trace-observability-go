.PHONY: all build test race vet demo tidy clean

all: build vet race

build:
	go build ./...

test:
	go test ./...

# 竞态检测 + 高重复次数，重点压并发/取消/队列时序
race:
	go test -race -count=3 ./...

stress:
	go test -race -count=20 ./tracing/ ./export/ ./pipeline/

vet:
	go vet ./...

demo:
	go run ./cmd/demo

tidy:
	go mod tidy

clean:
	rm -rf data
