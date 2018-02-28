/*Extract, Transform, and Load data from ElasitcSearch into DGraph*/
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"time"

	dclient "github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/protos/api"
	"github.com/korovkin/limiter"
	"google.golang.org/grpc"
	elastic "gopkg.in/olivere/elastic.v5"
)

type elasticUserRelationship struct {
	LastUpdate   time.Time `json:"last_update"`
	FromPersonID string    `json:"from_person_id"`
	ToPersonID   string    `json:"to_person_id"`
	Stats        stats     `json:"stats"`
}

type stats struct {
	RawScoreIn  int `json:"raw_score_in"`
	RawScoreOut int `json:"raw_score_out"`
}

func (ur elasticUserRelationship) String() string {
	str := `<ElasticRelationship
	LastUpdate=%s
	ToPersonId=%s
	FromPersonId=%s
	RawScoreIn=%d
	RawScoreOut=%d
>`

	return fmt.Sprintf(
		str,
		ur.LastUpdate.Unix(),
		ur.ToPersonID,
		ur.FromPersonID,
		ur.Stats.RawScoreIn,
		ur.Stats.RawScoreOut,
	)
}

type person struct {
	Name          string `json:"name,omitempty"`
	TeamID        string `json:"team_id,omitempty"`
	PersonID      string `json:"person_id,omitempty"`
	HasMember     string `json:"has_member,omitempty"`
	ShareHash     string `json:"_share_hash_,omitempty"`
	HasConnection string `json:"has_connection,omitempty"`
}

type hasConnection struct {
	UID   string  `json:"uid"`
	Score float64 `json:"has_connection|score"`
}

type dgRelationship struct {
	UID           string          `json:"uid"`
	HasConnection []hasConnection `json:"has_connection"`
}

func (r dgRelationship) String() string {
	str := `<DgraphRelationship
	FromPersonUID=%s
	ToPersonUID=%s
	Score=%f
>`

	return fmt.Sprintf(
		str,
		r.UID,
		r.HasConnection[0].UID,
		r.HasConnection[0].Score,
	)
}

type relationshipResp struct {
	FindRelationship []dgRelationship `json:"find_relationship"`
}

var (
	elasticServer = flag.String("elastic", "", "elasticsearch server, e.g. http://192.168.128.237:9200")
	dgraphServer  = flag.String("dgraph", "", "dgraph server, e.g. dgraph1.nj.us.nomail.net:9080")
	watermark     = flag.String("watermark", "0", "watermark timestamp where we should start searching in elasticsearch")
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

	channel := make(chan elasticUserRelationship)
	defer close(channel)

	// extract
	go extractElasticUserRelationships(client, *watermark, channel)

	limit := limiter.NewConcurrencyLimiter(*maxThreads)
	for elasticRelationship := range channel {
		limit.Execute(func() {
			transformAndLoad(elasticRelationship, dg)
		})
	}
	limit.Wait()
}

func retryFindOrCreatePerson(
	personID string,
	dg *dclient.Dgraph,
	retries int,
	curRetries int,
) (string, error) {
	fromPersonUID, err := findOrCreatePerson(
		personID,
		dg,
	)

	if err != nil {
		if err.Error() != "Transaction has been aborted. Please retry." {
			return "", err
		}

		if curRetries < retries {
			curRetries++
			dur := 300 * time.Duration(curRetries) * random(1, 30)
			time.Sleep(dur * time.Millisecond)
			return retryFindOrCreatePerson(personID, dg, retries, curRetries)
		}
	}

	return fromPersonUID, nil
}

func transformAndLoad(e elasticUserRelationship, dg *dclient.Dgraph) {
	fromPersonUID, err := retryFindOrCreatePerson(
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

	toPersonUID, err := retryFindOrCreatePerson(
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
	relationships := transformElasticToDgraph(
		&e,
		fromPersonUID,
		toPersonUID,
	)
	// load
	loadIntoDgraph(relationships, dg)
}

func extractElasticUserRelationships(
	client *elastic.Client,
	watermark string,
	channel chan elasticUserRelationship,
) {
	termQuery := elastic.NewRangeQuery("last_update").
		Gte(watermark)

	scroller := client.Scroll().
		Index("user_relationship").
		Query(termQuery).
		Sort("last_update", true).
		Pretty(true).
		Size(250)

	var docs int64 = 1
	for {
		res, err := scroller.Do(context.TODO())

		if err == io.EOF {
			break
		}

		total := res.TotalHits()

		for _, hit := range res.Hits.Hits {
			var ur elasticUserRelationship
			err := json.Unmarshal(*hit.Source, &ur)
			if err != nil {
				fmt.Println(err)
				continue
			}

			if docs%1000 == 0 {
				ratio := (float64(docs) / float64(total)) * 100.0
				fmt.Printf(
					"%f percent (%d / %d), watermark: %d\n",
					ratio,
					docs,
					total,
					ur.LastUpdate.Unix(),
				)
			}

			docs++
			channel <- ur
		}
	}
}

func transformElasticToDgraph(
	elastic *elasticUserRelationship,
	FromPersonUID string,
	ToPersonUID string,
) []dgRelationship {
	relationIn := dgRelationship{
		UID: FromPersonUID,
		HasConnection: []hasConnection{
			hasConnection{
				UID:   ToPersonUID,
				Score: float64(elastic.Stats.RawScoreIn),
			},
		},
	}

	relationOut := dgRelationship{
		UID: ToPersonUID,
		HasConnection: []hasConnection{
			hasConnection{
				UID:   FromPersonUID,
				Score: float64(elastic.Stats.RawScoreOut),
			},
		},
	}

	return []dgRelationship{
		relationIn,
		relationOut,
	}
}

func random(min, max int) time.Duration {
	rand.Seed(time.Now().Unix())
	return time.Duration(rand.Intn(max-min) + min)
}

func retryCreateOrUpdateRelationship(
	r *dgRelationship,
	dg *dclient.Dgraph,
	retries int,
	curRetries int,
) error {
	err := createOrUpdateRelationship(r, dg)
	if err != nil {
		if err.Error() != "Transaction has been aborted. Please retry." {
			return err
		}

		if curRetries < retries {
			curRetries++
			dur := 300 * time.Duration(curRetries) * random(1, 30)
			time.Sleep(dur * time.Millisecond)
			return retryCreateOrUpdateRelationship(r, dg, retries, curRetries)
		}

		return err
	}

	return nil
}

func loadIntoDgraph(relationships []dgRelationship, dg *dclient.Dgraph) {
	for _, relation := range relationships {
		var r = relation
		err := retryCreateOrUpdateRelationship(&r, dg, 3, 0)
		if err != nil {
			fmt.Println("Error creating relationship")
			fmt.Println(err)
		}
	}
}

func createOrUpdateRelationship(relation *dgRelationship, dg *dclient.Dgraph) error {
	res, err := findRelationship(relation, dg)
	if err != nil {
		return err
	}

	if len(res) == 0 {
		_, err := createRelationship(relation, dg)
		if err != nil {
			return err
		}
		return nil
	}

	storedRelation := res[0]
	if relation.HasConnection[0].Score > storedRelation.HasConnection[0].Score {
		err := updateRelationship(relation, dg)
		if err != nil {
			return err
		}
	}

	return nil
}

func findRelationship(
	relation *dgRelationship,
	dg *dclient.Dgraph,
) ([]dgRelationship, error) {
	ctx := context.Background()
	txn := dg.NewTxn()
	defer txn.Discard(ctx)
	queryStr := `query find_relationship($from_person_id: string) {
		find_relationship(func: uid($from_person_id)) @cascade {
			uid
			has_connection  @filter(uid(%s)) @facets(score) {
				uid
			}
		}
	}`
	toPersonID := relation.HasConnection[0].UID
	query := fmt.Sprintf(queryStr, toPersonID)

	var vars = map[string]string{
		"$from_person_id": relation.UID,
	}

	resp, err := txn.QueryWithVars(
		ctx,
		query,
		vars,
	)

	if err != nil {
		return []dgRelationship{}, err
	}

	var data relationshipResp
	if err := json.Unmarshal(resp.GetJson(), &data); err != nil {
		return []dgRelationship{}, err
	}

	return data.FindRelationship, nil
}

func createRelationship(
	relation *dgRelationship,
	dg *dclient.Dgraph,
) (string, error) {
	mu := &api.Mutation{
		CommitNow: true,
	}

	pb, err := json.Marshal(relation)
	if err != nil {
		return "", err
	}

	mu.SetJson = pb
	ctx := context.Background()
	txn := dg.NewTxn()
	assigned, err := txn.Mutate(ctx, mu)
	if err != nil {
		return "", err
	}

	uid := assigned.Uids["blank-0"]
	return uid, nil
}

func updateRelationship(
	relation *dgRelationship,
	dg *dclient.Dgraph,
) error {
	ctx := context.Background()
	txn := dg.NewTxn()
	defer txn.Discard(ctx)
	pb, err := json.Marshal(relation)
	if err != nil {
		return err
	}

	mu := &api.Mutation{
		CommitNow: true,
	}
	mu.SetJson = pb

	_, err = txn.Mutate(
		context.Background(),
		mu,
	)

	return err
}

func findOrCreatePerson(
	personID string,
	dg *dclient.Dgraph,
) (string, error) {
	uid, err := findPersonUID(personID, dg)
	if err != nil {
		if err.Error() != "person not found" {
			return "", err
		}

		newUID, nErr := createPerson(personID, dg)
		if nErr != nil {
			return "", nErr
		}

		return newUID, nil
	}

	return uid, nil
}

func findPersonUID(personID string, dg *dclient.Dgraph) (string, error) {
	ctx := context.Background()
	txn := dg.NewTxn()
	defer txn.Discard(ctx)
	const query = `query find_uid($person_id: string)
		{
			find_uid(func: eq(person_id, $person_id)) {uid: uid}
		}
	`

	resp, err := txn.QueryWithVars(
		ctx,
		query,
		map[string]string{"$person_id": personID},
	)

	if err != nil {
		return "", err
	}

	var data map[string][]map[string]string
	if err := json.Unmarshal(resp.GetJson(), &data); err != nil {
		return "", err
	}

	if len(data["find_uid"]) > 0 {
		return data["find_uid"][0]["uid"], nil
	}

	return "", errors.New("person not found")
}

func createPerson(personID string, dg *dclient.Dgraph) (string, error) {
	mu := &api.Mutation{
		CommitNow: true,
	}

	p := person{
		Name:     "",
		PersonID: personID,
	}

	pb, err := json.Marshal(p)
	if err != nil {
		return "", err
	}

	mu.SetJson = pb
	ctx := context.Background()
	txn := dg.NewTxn()
	defer txn.Discard(ctx)
	assigned, err := txn.Mutate(ctx, mu)
	if err != nil {
		return "", err
	}

	uid := assigned.Uids["blank-0"]
	return uid, nil
}
