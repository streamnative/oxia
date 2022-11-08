
.PHONY: build
build: proto
	go build -v -o oxia ./cmd

test: build
	go test -race ./...

clean:
	rm -f oxia
	rm -f */*.pb.go

docker: docker_arm docker_x86

docker_arm:
	env GOOS=linux GOARCH=arm64 go build -o oxia ./cmd
	docker build --platform arm64 -t oxia:latest .

docker_x86:
	env GOOS=linux GOARCH=amd64 go build -o oxia ./cmd
	docker build --platform x86_64 -t oxia:latest-x86_64 .

.PHONY: proto
proto:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative */*.proto

proto_format:
	#brew install clang-format
	clang-format -i --style=Google client/client.proto

proto_lint:
	#go install github.com/yoheimuta/protolint/cmd/protolint
	protolint lint client/client.proto

proto_doc:
	#go install github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc
	protoc --doc_out=docs/proto --doc_opt=markdown,client.md client/client.proto

proto_quality: proto_format proto_lint proto_doc
