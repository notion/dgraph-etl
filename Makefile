prod:
	GOOS=linux GOARCH=amd64 go build -o dgraph_etl_linux ./main.go