build:
	go build -o ./etl ./cmd/etl

prod:
	GOOS=linux GOARCH=amd64 go build -o dgraph_etl_linux ./cmd/etl
