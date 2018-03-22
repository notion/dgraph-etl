/*Package etl extracts, transforms, and loads data from ElasitcSearch into DGraph*/
package main

import (
	"flag"
	"fmt"

	dclient "github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/protos/api"
	"github.com/korovkin/limiter"
	etl "github.com/neurosnap/dgraph-etl"
	"google.golang.org/grpc"
	elastic "gopkg.in/olivere/elastic.v5"
)

var (
	elasticServer = flag.String("elastic", "", "elasticsearch server, e.g. http://192.168.128.237:9200")
	dgraphServer  = flag.String("dgraph", "", "dgraph server, e.g. dgraph1.nj.us.nomail.net:9080")
	watermark     = flag.Int64("watermark", 0, "watermark timestamp where we should start searching in elasticsearch")
	maxThreads    = flag.Int("max-threads", 1000, "maximum number of goroutines running at once")
)

func main() {
	flag.Parse()

	if *elasticServer == "" {
		panic("elastic flag is required")
	}

	if *dgraphServer == "" {
		panic("dgraph flag is required")
	}

	fmt.Printf("Connecting to elasticsearch server: %s\n", *elasticServer)

	client, err := elastic.NewClient(elastic.SetURL(*elasticServer))
	if err != nil {
		panic(err)
	}
	defer client.Stop()

	fmt.Printf("Connecting to dgraph server: %s\n", *dgraphServer)

	conn, err := grpc.Dial(*dgraphServer, grpc.WithInsecure())
	if err != nil {
		panic("Could not connect to gRPC server")
	}
	defer conn.Close()

	dg := dclient.NewDgraphClient(api.NewDgraphClient(conn))

	channel := make(chan etl.ElasticUserRelationship)
	defer close(channel)

	// extract
	go etl.ExtractElasticUserRelationships(client, *watermark, channel)

	limit := limiter.NewConcurrencyLimiter(*maxThreads)
	for elasticRelationship := range channel {
		limit.Execute(func() {
			transformAndLoad(elasticRelationship, dg)
		})
	}
	limit.Wait()
}

func transformAndLoad(e etl.ElasticUserRelationship, dg *dclient.Dgraph) {
	fromPersonUID, err := etl.RetryFindOrCreatePerson(
		e.FromPersonID,
		dg,
		3,
		0,
	)
	if err != nil {
		fmt.Println("Error creating `from` person")
		fmt.Println(err)
		return
	}

	toPersonUID, err := etl.RetryFindOrCreatePerson(
		e.ToPersonID,
		dg,
		3,
		0,
	)
	if err != nil {
		fmt.Println("Error creating `to` person")
		fmt.Println(err)
		return
	}

	// transform
	relationships := etl.TransformElasticToDgraph(
		&e,
		fromPersonUID,
		toPersonUID,
	)
	// load
	loadIntoDgraph(relationships, dg)
}

func loadIntoDgraph(relationships []etl.DgRelationship, dg *dclient.Dgraph) {
	for _, relation := range relationships {
		var r = relation
		err := etl.RetryCreateOrUpdateRelationship(&r, dg, 4, 0)
		if err != nil {
			fmt.Println("Error creating relationship")
			fmt.Println(err)
		}
	}
}
