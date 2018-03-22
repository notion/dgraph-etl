package dgraphetl

import (
	"database/sql"
	"errors"
	"fmt"
	"math/big"
)

type MysqlShard struct {
	Low    big.Int
	High   big.Int
	Client *sql.DB
	Stmt   *sql.Stmt
}

func PrepareIsTroveUserStmt(client *sql.DB) (*sql.Stmt, error) {
	query := "SELECT COUNT(*) FROM user WHERE person_id_user=?"
	return client.Prepare(query)
}

func IsTroveUser(personID *big.Int, shards []*MysqlShard) (bool, error) {
	var shardServer *MysqlShard

	for _, shard := range shards {
		lowGTE := personID.Cmp(&shard.Low) == 1 || personID.Cmp(&shard.Low) == 0
		highLTE := personID.Cmp(&shard.High) == -1 || personID.Cmp(&shard.High) == 0

		if lowGTE && highLTE {
			shardServer = shard
			break
		}
	}

	if shardServer == nil {
		return false, errors.New(fmt.Sprintf("Could not find shard for %s\n", personID.String()))
	}

	stmt := shardServer.Stmt
	count := 0
	rows, err := stmt.Query(personID.String())
	if err != nil {
		return false, err
	}

	for rows.Next() {
		err := rows.Scan(&count)
		if err != nil {
			return false, err
		}
		break
	}

	rows.Close()

	return count > 0, nil
}

func GetTeamMembers(client *sql.DB) (map[string][]string, error) {
	teams := make(map[string][]string)
	rows, err := client.Query("SELECT team_id, person_id FROM team_member")
	if err != nil {
		return teams, err
	}

	for rows.Next() {
		var teamID string
		var personID string

		if err := rows.Scan(&teamID, &personID); err != nil {
			fmt.Println(err)
		}

		if _, ok := teams[teamID]; !ok {
			teams[teamID] = []string{}
		}

		teams[teamID] = append(teams[teamID], personID)
	}

	return teams, nil
}
