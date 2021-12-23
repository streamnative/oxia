
build:
	go build -o oxia ./server
	go build -o oxia-client ./client
	go build -o oxia-operator ./operator

clean:
	rm -f oxia oxia-client oxia-operator

proto:
	 protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/*.proto


.PHONY: proto

