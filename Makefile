
.PHONY: build
build: proto
	go build -v -o oxia ./cmd

test: build
	go test -race ./...

clean:
	rm -f oxia
	rm -f */*.pb.go

docker:
	docker build -t oxia:latest .

docker_multi_arch:
	docker buildx build --platform linux/x86_64,linux/arm64 -t oxia:latest .

.PHONY: proto
proto:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/*.proto
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative coordination/*.proto
