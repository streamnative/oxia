
build:
	go build -o oxia ./server
	go build -o oxia-client ./client
	go build -o oxia-operator ./operator

clean:
	rm -f oxia oxia-client oxia-operator

docker: docker_arm docker_x86

docker_arm:
	env GOOS=linux GOARCH=arm64 go build -o oxia ./server
	docker build --platform arm64 -t oxia:latest .

docker_x86:
	env GOOS=linux GOARCH=amd64 go build -o oxia ./server
	docker build --platform x86_64 -t oxia:latest-x86_64 .

proto:
	 protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/*.proto


.PHONY: proto

