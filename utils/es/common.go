package es

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"reflect"
	"strings"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

var batchSize = 5000

// PerformESInsert 执行 es 批量 insert 操作
// TODO: documents 数量达到一定程度时分批插入
func PerformESInsert(index string, documents []interface{}, esClient *elasticsearch.Client) error {
	if len(documents) == 0 {
		log.Println("documents is empty")
		return nil
	}
	requestBody, err := getInsertRequestBody(index, documents)
	if err != nil {
		log.Printf("Error getting request body: %s", err)
		return err
	}
	return PerformESBulk(index, requestBody, esClient)
}

// PerformESUpsert 执行 es 批量 upsert 操作
// update 时是按字段更新
// TODO: documents 数量达到一定程度时分批插入
func PerformESUpsert(index string, documents []interface{}, esClient *elasticsearch.Client) error {
	requestBody, err := getUpsertRequestBody(index, documents)
	if err != nil {
		log.Printf("Error getting request body: %s", err)
		return err
	}
	return PerformESBulk(index, requestBody, esClient)
}

// PerformESIndex Indexes the specified document. If the document exists, replaces the document and increments the version.
func PerformESIndex(index string, documents []interface{}, esClient *elasticsearch.Client) error {
	requestBody, err := getIndexRequestBody(index, documents)
	if err != nil {
		log.Printf("Error getting request body: %s", err)
		return err
	}
	return PerformESBulk(index, requestBody, esClient)
}

// DeleteESIndex ...
func DeleteESIndex(index string, esClient *elasticsearch.Client) error {
	log.Print("delete ES all documents. Collection: ", index)

	indexes := []string{index}
	req := esapi.IndicesDeleteRequest{
		Index: indexes,
	}

	// Perform the request with the client.
	res, err := req.Do(context.Background(), esClient)
	if err != nil {
		log.Printf("delete ES all documents, error getting response: %s", err)
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		log.Printf("delete ES all documents, [%s] error indexing document", res.Status())
		return err
	}

	log.Print("delete ES all documents. Collection: ", index, ". Done")

	return nil
}

// PerformESDelete 执行 es 批量 delete 操作
func PerformESDelete(index string, ids []string, esClient *elasticsearch.Client) error {

	log.Print("perform es document delete. Index: ", index, ". Total: ", len(ids))
	for i := 0; i < len(ids); i += batchSize {
		endIndex := i + batchSize
		if endIndex > len(ids) {
			endIndex = len(ids)
		}
		var bodyBuf bytes.Buffer
		for _, id := range ids[i:endIndex] {
			deleteHeader :=
				map[string]interface{}{
					"delete": map[string]interface{}{
						"_index": index,
						"_id":    id,
					},
				}
			header, err := json.Marshal(deleteHeader)
			if err != nil {
				log.Fatal(err)
				return err
			}
			bodyBuf.Write(header)
			bodyBuf.WriteByte('\n')
		}
		err := PerformESBulk(index, bodyBuf.String(), esClient)
		if err != nil {
			log.Fatal(err)
			return err
		}
	}

	return nil
}

// 组装 es 批量插入语句
func getInsertRequestBody(index string, documents []interface{}) (string, error) {
	var bodyBuf bytes.Buffer
	for _, document := range documents {
		documentValue := reflect.ValueOf(document).Elem()
		createHeader :=
			map[string]interface{}{
				"create": map[string]interface{}{
					"_index": index,
					"_id":    documentValue.FieldByName("ID").String(),
					"_type":  "_doc",
				},
			}
		header, err := json.Marshal(createHeader)
		if err != nil {
			// TODO: LOG
			return "", err
		}
		bodyBuf.Write(header)
		bodyBuf.WriteByte('\n')
		content, err := json.Marshal(document)
		if err != nil {
			// TODO: LOG
			return "", err
		}
		bodyBuf.Write(content)
		bodyBuf.WriteByte('\n')
	}
	return bodyBuf.String(), nil
}

func getUpsertRequestBody(index string, documents []interface{}) (string, error) {
	var bodyBuf bytes.Buffer
	for _, document := range documents {
		documentValue := reflect.ValueOf(document).Elem()
		upsertHeader :=
			map[string]interface{}{
				"update": map[string]interface{}{
					"_index": index,
					"_id":    documentValue.FieldByName("ID").String(),
					"_type":  "_doc",
					// 失败重试 3 次
					"retry_on_conflict": 3,
				},
			}
		header, err := json.Marshal(upsertHeader)
		if err != nil {
			// TODO: LOG
			return "", err
		}
		bodyBuf.Write(header)
		bodyBuf.WriteByte('\n')

		upsertBody :=
			map[string]interface{}{
				"doc":           document,
				"doc_as_upsert": true,
			}
		content, err := json.Marshal(upsertBody)
		if err != nil {
			// TODO: LOG
			return "", err
		}
		bodyBuf.Write(content)
		bodyBuf.WriteByte('\n')
	}
	return bodyBuf.String(), nil
}

// 详见： https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html
func getIndexRequestBody(index string, documents []interface{}) (string, error) {
	var bodyBuf bytes.Buffer
	for _, document := range documents {
		documentValue := reflect.ValueOf(document).Elem()
		indexHeader :=
			map[string]interface{}{
				"index": map[string]interface{}{
					"_index": index,
					"_id":    documentValue.FieldByName("ID").String(),
				},
			}
		header, err := json.Marshal(indexHeader)
		if err != nil {
			// TODO: LOG
			return "", err
		}
		bodyBuf.Write(header)
		bodyBuf.WriteByte('\n')
		content, err := json.Marshal(document)
		if err != nil {
			// TODO: LOG
			return "", err
		}
		bodyBuf.Write(content)
		bodyBuf.WriteByte('\n')
	}
	return bodyBuf.String(), nil
}

// PerformESBulk ES 批量操作接口
// 详见：https://www.elastic.co/guide/en/elasticsearch/reference/master/docs-bulk.html
func PerformESBulk(index string, requestBody string, esClient *elasticsearch.Client) error {
	// Set up the request object.
	req := esapi.BulkRequest{
		Index:   index,
		Body:    strings.NewReader(requestBody),
		Refresh: "false",
		Pretty:  false,
	}

	// Perform the request with the client.
	res, err := req.Do(context.Background(), esClient)
	if err != nil {
		log.Printf("Error getting response: %s", err)
		return err
	}
	defer res.Body.Close()
	if res.IsError() {
		log.Printf("[%s] Error indexing document", res.Status())
		return err
	}
	// Deserialize the response into a map.
	var r map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Printf("Error parsing the response body: %s", err)
	}
	return nil
}
