package dgraphetl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"time"

	dclient "github.com/dgraph-io/dgraph/client"
	"github.com/dgraph-io/dgraph/protos/api"
)

type Person struct {
	Name          string `json:"name,omitempty"`
	TeamID        string `json:"team_id,omitempty"`
	PersonID      string `json:"person_id,omitempty"`
	HasMember     string `json:"has_member,omitempty"`
	ShareHash     string `json:"_share_hash_,omitempty"`
	HasConnection string `json:"has_connection,omitempty"`
}

type HasConnection struct {
	UID   string  `json:"uid"`
	Score float64 `json:"has_connection|score"`
}

type DgRelationship struct {
	UID           string          `json:"uid"`
	HasConnection []HasConnection `json:"has_connection"`
}

func (r DgRelationship) String() string {
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

type RelationshipResp struct {
	FindRelationship []DgRelationship `json:"find_relationship"`
}

func RetryFindOrCreatePerson(
	personID string,
	dg *dclient.Dgraph,
	retries int,
	curRetries int,
) (string, error) {
	fromPersonUID, err := FindOrCreatePerson(
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
			return RetryFindOrCreatePerson(personID, dg, retries, curRetries)
		}
	}

	return fromPersonUID, nil
}

func random(min, max int) time.Duration {
	rand.Seed(time.Now().Unix())
	return time.Duration(rand.Intn(max-min) + min)
}

func RetryCreateOrUpdateRelationship(
	r *DgRelationship,
	dg *dclient.Dgraph,
	retries int,
	curRetries int,
) error {
	err := CreateOrUpdateRelationship(r, dg)
	if err != nil {
		if err.Error() != "Transaction has been aborted. Please retry." {
			return err
		}

		if curRetries < retries {
			curRetries++
			dur := 300 * time.Duration(curRetries) * random(1, 30)
			time.Sleep(dur * time.Millisecond)
			return RetryCreateOrUpdateRelationship(r, dg, retries, curRetries)
		}

		return err
	}

	return nil
}

func CreateOrUpdateRelationship(relation *DgRelationship, dg *dclient.Dgraph) error {
	res, err := FindRelationship(relation, dg)
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
		err := UpdateRelationship(relation, dg)
		if err != nil {
			return err
		}
	}

	return nil
}

func FindRelationship(
	relation *DgRelationship,
	dg *dclient.Dgraph,
) ([]DgRelationship, error) {
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
		return []DgRelationship{}, err
	}

	var data RelationshipResp
	if err := json.Unmarshal(resp.GetJson(), &data); err != nil {
		return []DgRelationship{}, err
	}

	return data.FindRelationship, nil
}

func createRelationship(
	relation *DgRelationship,
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

func UpdateRelationship(
	relation *DgRelationship,
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

func FindOrCreatePerson(
	personID string,
	dg *dclient.Dgraph,
) (string, error) {
	uid, err := FindPersonUID(personID, dg)
	if err != nil {
		if err.Error() != "person not found" {
			return "", err
		}

		newUID, nErr := CreatePerson(personID, dg)
		if nErr != nil {
			return "", nErr
		}

		return newUID, nil
	}

	return uid, nil
}

func FindPersonUID(personID string, dg *dclient.Dgraph) (string, error) {
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

func CreatePerson(personID string, dg *dclient.Dgraph) (string, error) {
	mu := &api.Mutation{
		CommitNow: true,
	}

	p := Person{
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

func TransformElasticToDgraph(
	elastic *ElasticUserRelationship,
	FromPersonUID string,
	ToPersonUID string,
) []DgRelationship {
	relationIn := DgRelationship{
		UID: FromPersonUID,
		HasConnection: []HasConnection{
			HasConnection{
				UID:   ToPersonUID,
				Score: float64(elastic.Stats.RawScoreIn),
			},
		},
	}

	relationOut := DgRelationship{
		UID: ToPersonUID,
		HasConnection: []HasConnection{
			HasConnection{
				UID:   FromPersonUID,
				Score: float64(elastic.Stats.RawScoreOut),
			},
		},
	}

	return []DgRelationship{
		relationIn,
		relationOut,
	}
}
