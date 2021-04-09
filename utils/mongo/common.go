package mongo

import (
	"context"
	"errors"
	"log"
	"reflect"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

var batchSize = 20000

// PerformMongoDBInsert ...
func PerformMongoDBInsert(documents []interface{}, collection *mongo.Collection) error {

	if collection == nil {
		return errors.New("collection can not be nil")
	}
	if len(documents) != 0 {
		_, err := collection.InsertMany(context.TODO(), documents)
		if err != nil {
			log.Fatal(err)
			return err
		}
	}
	return nil
}

// PerformMongoDBUpsert 批量更新插入操作,当 entity_id,entity_type 的联合主键记录存在时，更新记录，否则添加记录
// TODO: mongo document 变化后未测试
func PerformMongoDBUpsert(documents []interface{}, collection *mongo.Collection) error {

	if collection == nil {
		return errors.New("collection can not be nil")
	}
	if len(documents) == 0 {
		return nil
	}
	writeModels := []mongo.WriteModel{}
	upsertFlag := true
	for i := range documents {
		value := reflect.ValueOf(documents[i]).Elem()
		filter := make(bson.M, 0)
		update := make(bson.M, 0)
		filter["entity_document.entity_id"] = value.FieldByName("EntityID").Interface()
		filter["entity_document.entity_type"] = value.FieldByName("EntityType").Interface()
		update["$set"] = documents[i]
		updateOneModel := mongo.UpdateOneModel{
			Filter: filter,
			Update: update,
			Upsert: &upsertFlag,
		}
		writeModels = append(writeModels, &updateOneModel)
	}
	if _, err := collection.BulkWrite(context.TODO(), writeModels); err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}

// PerformMongoDBDelete ...
func PerformMongoDBDelete(ids []string, collection *mongo.Collection) error {

	log.Println("perform mongo document delete. Collection: ", collection.Name(), ". Total: ", len(ids))
	if collection == nil {
		return errors.New("collection can not be nil")
	}
	for i := 0; i < len(ids); i += batchSize {
		endIndex := i + batchSize
		if endIndex > len(ids) {
			endIndex = len(ids)
		}
		filter := bson.M{
			"ids": bson.M{
				"$in": ids,
			},
		}
		if _, err := collection.DeleteMany(context.TODO(), filter); err != nil {
			log.Fatal(err)
			return err
		}
	}

	return nil
}

// DeleteMongoCollection ...
func DeleteMongoCollection(collection *mongo.Collection) error {

	if collection == nil {
		return errors.New("collection can not be nil")
	}

	log.Println("delete mongo all documents. Collection: ", collection.Name())

	filter := bson.M{}
	deleteResult, err := collection.DeleteMany(context.TODO(), filter)
	if err != nil {
		log.Fatal(err)
		return err
	}

	log.Println("delete mongo all documents. Collection: ", collection.Name(), ". DeletedCount: ", deleteResult.DeletedCount)

	return nil
}
