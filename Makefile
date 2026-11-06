.PHONY: all proto build test run-server run-client docker-up docker-down k8s-deploy k8s-delete clean

all: proto test build

proto:
	protoc -I proto --go_out=pkg/pb --go_opt=paths=source_relative --go-grpc_out=pkg/pb --go-grpc_opt=paths=source_relative proto/chat.proto

build:
	go build -o bin/server ./cmd/server
	go build -o bin/client ./cmd/client

test:
	go test -v -race ./tests/...

run-server:
	go run ./cmd/server/main.go -port 50051 -use-memory-broker=true

run-client:
	go run ./cmd/client/main.go -addr localhost:50051

docker-up:
	docker compose -f deployments/docker-compose.yml up --build -d

docker-down:
	docker compose -f deployments/docker-compose.yml down -v

k8s-deploy:
	kubectl apply -k deployments/k8s

k8s-delete:
	kubectl delete -k deployments/k8s

clean:
	rm -rf bin/

