
build:
	go build -o oxia ./server
	go build -o oxia-client ./client

clean:
	rm -f oxia oxia-client

proto:
	 protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/*.proto


.PHONY: proto

