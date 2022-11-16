
.PHONY: build
build: proto
	go build -v -o bin/oxia ./cmd

test: build
	go test -cover -race ./...

clean:
	rm -f bin/oxia
	rm -f */*.pb.go

docker:
	docker build -t oxia:latest .

docker_multi_arch:
	docker buildx build --platform linux/x86_64,linux/arm64 -t oxia:latest .

.PHONY: proto
proto:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/*.proto

proto_format:
	#brew install clang-format
	clang-format -i --style=Google proto/*.proto

proto_lint:
	#go install github.com/yoheimuta/protolint/cmd/protoc-gen-protolint
	protoc --protolint_out=. --protolint_opt=config_dir_path=. proto/*.proto

proto_doc:
	#go install github.com/pseudomuto/protoc-gen-doc/cmd/protoc-gen-doc
	protoc --doc_out=docs/proto --doc_opt=markdown,proto.md proto/*.proto

proto_quality: proto_format proto_lint

fetch-tla-tools:
	mkdir -p tlaplus/.tools
	cd tlaplus/.tools && \
		wget https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar && \
		wget https://github.com/tlaplus/CommunityModules/releases/download/202211012231/CommunityModules-deps.jar


tla:
	cd tlaplus && \
		java -DTLA-Library=.tools/CommunityModules-deps.jar -jar .tools/tla2tools.jar OxiaReplication.tla