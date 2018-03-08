.PHONY: bulk etl

build-etl:
	go build -o ./etl ./cmd/etl

build-query:
	go build -o ./query ./cmd/query

build-bulk:
	go build -o ./bulk ./cmd/bulk


build: build-etl build-bulk

etl:
	GOOS=linux GOARCH=amd64 go build -o dgraph_etl_linux ./cmd/etl

bulk:
	GOOS=linux GOARCH=amd64 go build -o dgraph_bulk_linux ./cmd/bulk
