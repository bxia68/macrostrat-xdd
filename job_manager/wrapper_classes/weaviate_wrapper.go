package wrapper_classes

import (
	"context"
	"encoding/json"
	"github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"
	"log"
)

type ParagraphData struct {
	Additional struct {
		ID string `json:"id"`
	} `json:"_additional"`
	TopicList []string `json:"topic_list"`
}

type GraphQLData struct {
	Paragraphs []ParagraphData `json:"Paragraph"`
}

func GetBatchWithCursor(client *weaviate.Client, batchSize int, cursor string) (*models.GraphQLResponse, error) {
	get := client.GraphQL().Get().
		WithClassName("Paragraph").
		WithFields(graphql.Field{Name: "_additional { id }"}, graphql.Field{Name: "topic_list"}).
		WithLimit(batchSize)

	if cursor != "" {
		return get.WithAfter(cursor).Do(context.Background())
	}
	return get.Do(context.Background())
}

func GenerateFilteredBatch(client *weaviate.Client, batchSize int, cursor *string) ([]string, bool) {
	var batch []string
	for {
		response, err := GetBatchWithCursor(client, batchSize, *cursor)
		if err != nil || response.Errors != nil {
			errors, _ := json.Marshal(response.Errors)
			log.Fatalf("Error fetching data or GraphQL errors: %v", string(errors))
		}

		// parse response
		var data GraphQLData
		responseData, err := json.Marshal(response.Data["Get"])
		if err != nil {
			log.Fatalf("Error converting response data to byte slice: %v", err)
		}
		if err := json.Unmarshal(responseData, &data); err != nil {
			log.Fatalf("Error parsing GraphQL data: %v", err)
		}

		// filter to only geoarchive topics
		for _, paragraph := range data.Paragraphs {
			for _, topic := range paragraph.TopicList {
				if topic == "geoarchive" {
					batch = append(batch, paragraph.Additional.ID)
					continue
				}
			}
		}

		// update cursor
		*cursor = data.Paragraphs[len(data.Paragraphs)-1].Additional.ID

		// check if batch is full or if there are no more paragraphs left in weaviate
		if len(data.Paragraphs) < batchSize {
			return batch, true
		}
		if len(batch) >= batchSize {
			return batch, false
		}
	}
}
