package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math/big"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/korovkin/limiter"
	etl "github.com/neurosnap/dgraph-etl"
	elastic "gopkg.in/olivere/elastic.v5"
)

var (
	elasticServer      = flag.String("elastic", "", "elasticsearch server, e.g. http://192.168.128.237:9200")
	filename           = flag.String("out", "./triples.rdf", "rdf file name to output to")
	watermark          = flag.Int64("watermark", 0, "watermark timestamp where we should start searching in elasticsearch")
	mysqlServerUserOne = flag.String("mysql-user-one", "", "mysql server, e.g. user:password@host/dbname")
	mysqlServerUserTwo = flag.String("mysql-user-two", "", "mysql server, e.g. user:password@host/dbname")
	mysqlServerTeam    = flag.String("mysql-team", "", "mysql server, e.g. user:password@host/dbname")
	maxThreads         = flag.Int("max-threads", 6000, "maximum number of goroutines running at once")
)

type store struct {
	sync.RWMutex
	cache map[string]int
}

func NewStore() *store {
	return &store{
		cache: make(map[string]int),
	}
}

func (s *store) set(key string, value int) {
	s.Lock()
	defer s.Unlock()

	s.cache[key] = value
}

func (s *store) get(key string) (int, bool) {
	s.RLock()
	defer s.RUnlock()

	item, ok := s.cache[key]
	return item, ok
}

func main() {
	flag.Parse()

	f, err := os.Create(*filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if *mysqlServerUserOne == "" {
		panic("mysql-user-one flag is required")
	}

	if *mysqlServerUserTwo == "" {
		panic("mysql-user-two flag is required")
	}

	uMap := NewStore()
	rMap := NewStore()

	shards := connectToUserShards(
		*mysqlServerUserOne,
		*mysqlServerUserTwo,
	)

	buildRDFUserRelationships(
		f,
		shards,
		uMap,
		rMap,
		*elasticServer,
		*watermark,
		*maxThreads,
	)
	buildRDFTeamRelationships(f, shards, uMap, *mysqlServerTeam, *maxThreads)

	fmt.Println("File saved!")
}

func buildRDFTeamRelationships(
	f *os.File,
	shards []*etl.MysqlShard,
	uMap *store,
	mysqlServerTeam string,
	maxThreads int,
) {
	if mysqlServerTeam == "" {
		panic("mysql-team is required")
	}

	fmt.Printf("Connect to team mysql server: %s\n", mysqlServerTeam)
	mysqlClientTeam, err := sql.Open("mysql", mysqlServerTeam)
	if err != nil {
		panic(err)
	}
	mysqlClientTeam.SetMaxIdleConns(0)
	mysqlClientTeam.SetMaxOpenConns(500)
	mysqlClientTeam.SetConnMaxLifetime(time.Second * 10)

	teams, err := etl.GetTeamMembers(mysqlClientTeam)
	if err != nil {
		panic(err)
	}

	limit := limiter.NewConcurrencyLimiter(maxThreads)
	for teamID, members := range teams {
		fmt.Printf("Creating team: %s\n", teamID)
		f.Write([]byte(createRDFTeam(teamID)))

		for _, personID := range members {
			limit.Execute(func() {
				user := createRDFUser(personID, shards, uMap)
				fmt.Printf("Created person %s\n", personID)
				f.Write([]byte(strings.Join(user, "")))
				f.Write([]byte(createRDFTeamMember(teamID, personID)))
			})
		}
	}
	limit.Wait()
}

func buildRDFUserRelationships(
	f *os.File,
	shards []*etl.MysqlShard,
	uMap *store,
	rMap *store,
	elasticServer string,
	watermark int64,
	maxThreads int,
) {
	if elasticServer == "" {
		panic("elastic flag is required")
	}

	fmt.Printf("Connecting to elasticsearch server: %s\n", elasticServer)
	client, err := elastic.NewClient(elastic.SetURL(elasticServer))
	if err != nil {
		panic(err)
	}
	defer client.Stop()

	channel := make(chan etl.ElasticUserRelationship)
	go etl.ExtractElasticUserRelationships(client, watermark, channel)

	limit := limiter.NewConcurrencyLimiter(maxThreads)

	for elasticRelationship := range channel {
		limit.Execute(func() {
			userOne := createRDFUser(
				elasticRelationship.ToPersonID,
				shards,
				uMap,
			)
			f.Write([]byte(strings.Join(userOne, "")))

			userTwo := createRDFUser(
				elasticRelationship.FromPersonID,
				shards,
				uMap,
			)
			f.Write([]byte(strings.Join(userTwo, "")))

			relate := createRDFRelationships(&elasticRelationship)
			var finalRelation []string

			keyFrom := fmt.Sprintf(
				"%s-%s",
				elasticRelationship.FromPersonID,
				elasticRelationship.ToPersonID,
			)
			scoreIn := elasticRelationship.Stats.RawScoreIn
			if val, ok := rMap.get(keyFrom); ok {
				if scoreIn > val {
					rMap.set(keyFrom, scoreIn)
					finalRelation = append(finalRelation, relate[0])
				}
			} else {
				rMap.set(keyFrom, scoreIn)
				finalRelation = append(finalRelation, relate[0])
			}

			keyTo := fmt.Sprintf(
				"%s-%s",
				elasticRelationship.ToPersonID,
				elasticRelationship.FromPersonID,
			)
			scoreOut := elasticRelationship.Stats.RawScoreOut
			if val, ok := rMap.get(keyTo); ok {
				if scoreOut > val {
					rMap.set(keyTo, scoreOut)
					finalRelation = append(finalRelation, relate[1])
				}
			} else {
				rMap.set(keyTo, scoreOut)
				finalRelation = append(finalRelation, relate[1])
			}

			f.Write([]byte(strings.Join(finalRelation, "")))
		})
	}

	go func() {
		limit.Wait()
		close(channel)
	}()
}

func connectToUserShards(mysqlServerOne string, mysqlServerTwo string) []*etl.MysqlShard {
	fmt.Printf("Connect to user one mysql server: %s\n", mysqlServerOne)
	mysqlClientOne, err := sql.Open("mysql", mysqlServerOne)
	if err != nil {
		panic(err)
	}
	mysqlClientOne.SetMaxIdleConns(0)
	mysqlClientOne.SetMaxOpenConns(250)
	mysqlClientOne.SetConnMaxLifetime(time.Second * 10)
	shardOne := &etl.MysqlShard{
		Client: mysqlClientOne,
	}
	shardOne.Low.SetUint64(0)
	shardOne.High.SetUint64(9223372036854775807)

	fmt.Printf("Connect to user two mysql server: %s\n", mysqlServerTwo)
	mysqlClientTwo, err := sql.Open("mysql", mysqlServerTwo)
	if err != nil {
		panic(err)
	}
	mysqlClientTwo.SetMaxIdleConns(0)
	mysqlClientTwo.SetMaxOpenConns(250)
	mysqlClientTwo.SetConnMaxLifetime(time.Second * 10)
	shardTwo := &etl.MysqlShard{
		Client: mysqlClientTwo,
	}
	shardTwo.Low.SetUint64(9223372036854775808)
	shardTwo.High.SetUint64(18446744073709551615)

	prepareQuery, err := etl.PrepareIsTroveUserStmt(mysqlClientOne)
	if err != nil {
		panic(err)
	}
	shardOne.Stmt = prepareQuery

	prepareQuery, err = etl.PrepareIsTroveUserStmt(mysqlClientTwo)
	if err != nil {
		panic(err)
	}
	shardTwo.Stmt = prepareQuery

	shards := []*etl.MysqlShard{
		shardOne,
		shardTwo,
	}

	return shards
}

func createRDFTeam(teamID string) string {
	return fmt.Sprintf("_:%s <team_id> \"%s\" .\n", teamID, teamID)
}

func createRDFTeamMember(teamID string, personID string) string {
	return fmt.Sprintf("_:%s <has_member> _:%s .\n", teamID, personID)
}

func createRDFUser(
	personID string,
	shards []*etl.MysqlShard,
	uMap *store,
) []string {
	if _, ok := uMap.get(personID); ok {
		return []string{}
	}

	id := new(big.Int)
	id, ok := id.SetString(personID, 10)
	if !ok {
		return []string{}
	}

	isTroveUser, err := etl.IsTroveUser(
		id,
		shards,
	)
	if err != nil {
		fmt.Println(err)
	}

	isTroveUserText := "false"
	if isTroveUser {
		isTroveUserText = "true"
	}

	triples := []string{
		fmt.Sprintf("_:%s <person_id> \"%s\" .\n", personID, personID),
		fmt.Sprintf(
			"_:%s <is_trove_user> \"%s\"^^<xs:boolean> .\n",
			personID,
			isTroveUserText,
		),
	}

	uMap.set(personID, 1)
	// channel <- triples
	return triples
}

func createRDFRelationships(
	er *etl.ElasticUserRelationship,
) []string {
	return []string{
		fmt.Sprintf("_:%s <has_connection> _:%s (score=%d) .\n", er.ToPersonID, er.FromPersonID, er.Stats.RawScoreIn),
		fmt.Sprintf("_:%s <has_connection> _:%s (score=%d) .\n", er.FromPersonID, er.ToPersonID, er.Stats.RawScoreOut),
	}
}

func createRDFTeamRelationships() {}
