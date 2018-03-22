package dgraphetl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	elastic "gopkg.in/olivere/elastic.v5"
)

type ElasticUserRelationship struct {
	LastUpdate   time.Time `json:"last_update"`
	FromPersonID string    `json:"from_person_id"`
	ToPersonID   string    `json:"to_person_id"`
	Stats        stats     `json:"stats"`
}

type stats struct {
	RawScoreIn  int `json:"raw_score_in"`
	RawScoreOut int `json:"raw_score_out"`
}

func (ur ElasticUserRelationship) String() string {
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

func ExtractElasticUserRelationships(
	client *elastic.Client,
	watermark int64,
	channel chan ElasticUserRelationship,
) {
	defer close(channel)
	timestamp := time.Unix(watermark, 0)
	fmt.Printf("Starting from timestamp: %s\n", timestamp)
	termQuery := elastic.NewRangeQuery("last_update").
		Gte(timestamp)

	scroller := client.Scroll().
		Index("user_relationship").
		Query(termQuery).
		Sort("last_update", true).
		Pretty(true).
		Size(5000)

	var docs int64 = 1
	for {
		res, err := scroller.Do(context.TODO())

		if err == io.EOF {
			break
		}

		if err != nil {
			panic(err)
		}

		total := res.TotalHits()

		for _, hit := range res.Hits.Hits {
			/* if docs >= 10000 {
				return
			} */
			var ur ElasticUserRelationship
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
