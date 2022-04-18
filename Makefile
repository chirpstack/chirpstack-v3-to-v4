.PHONY: proto build

build:
	go build

proto:
	protoc --go_out=. --go_opt=paths=source_relative pbold/pb.proto
	protoc --go_out=. --go_opt=paths=source_relative pbnew/pb.proto
