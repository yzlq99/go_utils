package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v7"
	"github.com/pkg/errors"
)

// PerformESQuery es response body will be unmarshal to `response`, so `response` parameter must be a pointer
func PerformESQuery(request interface{}, response interface{}, index string, client *elasticsearch.Client) error {
	var err error

	var reqBody bytes.Buffer
	err = json.NewEncoder(&reqBody).Encode(request)
	if err != nil {
		err = fmt.Errorf("encode query failed, %v", err)
		return err
	}
	res, err := client.Search(
		client.Search.WithContext(context.Background()),
		client.Search.WithIndex(string(index)),
		client.Search.WithBody(&reqBody),
		client.Search.WithTrackTotalHits(true),
		client.Search.WithPretty(),
		client.Search.WithTimeout(10*time.Second),
	)
	if err != nil {
		err = fmt.Errorf("Error getting response: %s", err)
		return err
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&e); err != nil {
			err = fmt.Errorf("Error parsing the response body: %s", err)
		} else {
			err = fmt.Errorf("[%s] %s: %s", res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"])
		}
		return err
	}

	var builder strings.Builder
	buf := make([]byte, 256)
	for {
		n, err := res.Body.Read(buf)
		builder.Write(buf[:n])
		if err != nil {
			if err != io.EOF {
				err = fmt.Errorf("read response failed, %v", err)
				return err
			}
			break
		}
	}
	return json.Unmarshal([]byte(builder.String()), response)
}

// PerformESQueryAndBuildScroll ...
func PerformESQueryAndBuildScroll(query map[string]interface{}, index string, esClient *elasticsearch.Client) ([]map[string]interface{}, string, error) {

	startTime := time.Now()

	resultList := make([]map[string]interface{}, 0)
	var err error

	var reqBody bytes.Buffer
	err = json.NewEncoder(&reqBody).Encode(query)
	if err != nil {
		err = fmt.Errorf("encode query failed, %v", err)
		return resultList, "", errors.WithStack(err)
	}
	res, err := esClient.Search(
		esClient.Search.WithContext(context.Background()),
		esClient.Search.WithIndex(string(index)),
		esClient.Search.WithBody(&reqBody),
		esClient.Search.WithTrackTotalHits(true),
		esClient.Search.WithPretty(),
		esClient.Search.WithTimeout(5*60*time.Second),
		esClient.Search.WithScroll(time.Minute),
	)
	if err != nil {
		err = fmt.Errorf("Error getting response: %s", err)
		return resultList, "", errors.WithStack(err)
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&e); err != nil {
			err = fmt.Errorf("Error parsing the response body: %s", err)
		} else {
			err = fmt.Errorf("[%s] %s: %s", res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"])
		}
		return resultList, "", errors.WithStack(err)
	}

	result := make(map[string]interface{})
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		err = fmt.Errorf("Error parsing the response body: %s", err)
		return resultList, "", errors.WithStack(err)
	}
	resultList = append(resultList, result)

	hits := result["hits"].(map[string]interface{})["hits"].([]interface{})

	scrollID := ""
	if len(hits) == query["size"].(int) {
		scrollID = result["_scroll_id"].(string)
	}

	// test code
	if len(hits) > 0 {
		log.Println("")
		log.Println("---------------- First level of PerformESQueryAndBuildScroll ---------------")
		length := len(hits)
		if length > 1 {
			length = 1
		}
		resultJSON, _ := json.Marshal(hits[0:length])
		log.Printf("adam Build scroll, result[%+v]", string(resultJSON))
		log.Printf("adam Build scroll, len(hits): [%+v], query time[%+v]", len(hits), time.Now().Sub(startTime))
	}

	return resultList, scrollID, err
}

// PerformESQueryWithScroll ...
func PerformESQueryWithScroll(scrollID string, esClient *elasticsearch.Client) ([]map[string]interface{}, string, error) {

	if scrollID == "" {
		return nil, "", fmt.Errorf("=========================*************scrollID can not be empty in adam.PerformESQueryWithScroll")
	}

	resultList := make([]map[string]interface{}, 0)
	startTime := time.Now()

	res, err := esClient.Scroll(
		esClient.Scroll.WithContext(context.Background()),
		esClient.Scroll.WithPretty(),
		esClient.Scroll.WithScrollID(scrollID),
		esClient.Scroll.WithScroll(time.Minute),
	)
	if err != nil {
		err = fmt.Errorf("Error getting response: %s", err)
		return resultList, "", errors.WithStack(err)
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err = json.NewDecoder(res.Body).Decode(&e); err != nil {
			err = fmt.Errorf("Error parsing the response body: %s", err)
		} else {
			err = fmt.Errorf("[%s] %s: %s", res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"])
		}
		return resultList, "", errors.WithStack(err)
	}

	result := make(map[string]interface{})
	if err = json.NewDecoder(res.Body).Decode(&result); err != nil {
		err = fmt.Errorf("Error parsing the response body: %s", err)
		return resultList, "", errors.WithStack(err)
	}

	resultList = append(resultList, result)

	scrollID = ""
	hits := result["hits"].(map[string]interface{})["hits"].([]interface{})
	if len(hits) > 0 {
		scrollID = result["_scroll_id"].(string)
	}

	// test code
	if len(hits) > 0 {
		log.Println("---------------- Second level of PerformESQueryWithScroll ---------------")
		resultJSON, _ := json.Marshal(hits[0])
		log.Printf("adam PerformESQueryWithScroll， len(resultList)[%d], len(hits)[%d], query time: [%+v]",
			len(resultList), len(hits), time.Now().Sub(startTime))
		log.Printf("adam PerformESQueryWithScroll， hits[0]: [%+v]", string(resultJSON))
	}

	return resultList, scrollID, nil
}
