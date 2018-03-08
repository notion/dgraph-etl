package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/protos/api"
	"google.golang.org/grpc"
)

var (
	dgraph = flag.String("dgraph", "127.0.0.1:9080", "Dgraph server address")
	teamID = flag.String("teamID", "", "Team ID to look for")
)

func main() {
	flag.Parse()
	conn, err := grpc.Dial(*dgraph, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	dg := client.NewDgraphClient(api.NewDgraphClient(conn))

	query := fmt.Sprintf(`{
  var(func: eq(team_id, %s)){
    src as has_member
  }
  var(func: uid(src)){
    hop1 as has_connection @filter(NOT uid(src))
  }
  hop1_count(func: uid(hop1)){
	# person_id
    hop2 as has_connection @filter(NOT uid(hop1) AND NOT uid(src))
  }
  hop2_count(func: uid(hop2)){
    person_id
  }
}`, *teamID)

	resp, err := dg.NewTxn().Query(context.Background(), query)

	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Response: %s\n", resp.Json)
}
