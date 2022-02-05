package mgodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/kr/pretty"
	"github.com/mitchellh/mapstructure"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"gopkg.in/mgo.v2/bson"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Mongo es la instancia del cliente como tal
type Mongo struct {
	Client *mongo.Client
}

// Default Structures
type Filter struct {
	Filter bson.M
	GetAll *options.FindOptions
}

type Register struct {
	ID        primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	CreatedAt time.Time          `json:"created_at" bson:"created_at"`
	UpdatedAt time.Time          `json:"updated_at" bson:"updated_at"`
	DeletedAt time.Time          `json:"deleted_at" bson:"deleted_at"`
}
type RegisterUnix struct {
	ID        primitive.ObjectID `json:"id" bson:"_id,omitempty"`
	CreatedAt int64              `json:"created_at" bson:"created_at"`
	UpdatedAt int64              `json:"updated_at" bson:"updated_at"`
	DeletedAt int64              `json:"deleted_at" bson:"deleted_at"`
}

var (
	session *Mongo
	once    sync.Once
	// ErrBadFormat request-format incorrecto
	ErrBadFormat = errors.New("El parametro request-format debe ser un json de sólo strings")
)

// Get obtiene el cliente
func Get() *Mongo {
	return session
}

// GetFiltersAndOptions separa los filtros y parametros desde la URL
func GetFiltersAndOptions(r *http.Request, format map[string]string) (*Filter, error) {
	keys := r.URL.Query()
	filters := bson.M{}
	parameters := &options.FindOptions{}

	if format == nil && keys["request-format"] != nil {
		err := json.Unmarshal([]byte(keys["request-format"][0]), &format)
		if err != nil {
			return nil, ErrBadFormat
		}
		delete(keys, "request-format")
	}

	// Recorremos los parametros para separarlos
	for key, value := range keys {
		switch key {
		case "sort":
			sort := bson.M{}
			ks := strings.Split(value[0], ",")
			pretty.Println(ks)
			for _, tag := range ks {
				v := 1
				if tag[:1] == "-" {
					tag = tag[1:]
					v = -1
				}
				sort[tag] = v
			}
			parameters.Sort = sort
		case "limit":
			val, err := strconv.ParseInt(value[0], 10, 64)
			if err != nil {
				return nil, err
			}
			parameters.Limit = &val
		case "offset", "skip":
			val, err := strconv.ParseInt(value[0], 10, 64)
			if err != nil {
				return nil, err
			}
			parameters.Skip = &val
		default:
			if format == nil || format[key] == "" {
				filters[key] = value[0]
				continue
			}
			switch format[key] {
			case "int":
				val, err := strconv.Atoi(value[0])
				if err != nil {
					return nil, err
				}
				filters[key] = val
			case "bool":
				val, err := strconv.ParseBool(value[0])
				if err != nil {
					return nil, err
				}
				filters[key] = val
			case "uint":
				val, err := strconv.ParseUint(value[0], 10, 64)
				if err != nil {
					return nil, err
				}
				filters[key] = val
			default:
				filters[key] = value[0]
			}
		}
	}

	return &Filter{
		Filter: filters,
		GetAll: parameters,
	}, nil
}

// Configure Configurar el cliente
func Configure(username, password, host, port, authDB string, remote ...bool) {
	once.Do(func() {
		var err error
		sess, err := newMongoClient(username, password, host, port, authDB, remote...)
		if err != nil {
			log.Println("Problema con mongo")
			log.Fatal(err)
		}
		log.Println("Instanciamos mongo")
		session = sess
	})
}

// newMongoClient Crea un cliente (se conecta a mongo)
func newMongoClient(username, password, host, port, authDB string, remote ...bool) (*Mongo, error) {
	var uri string
	if len(remote) > 0 && remote[0] {
		uri = fmt.Sprintf("mongodb+srv://%s:%s@%s/%s?retryWrites=true&w=majority", username, password, host, authDB)
	} else {
		uri = "mongodb://"
		if username != "" || password != "" {
			uri += fmt.Sprintf("%s:%s@", username, password)
		}
		uri += fmt.Sprintf("%s:%s/", host, port)
		if authDB != "" {
			uri += authDB
		}
	}
	log.Println(uri)
	client, err := mongo.NewClient(options.Client().ApplyURI(uri))

	if err != nil {
		log.Println("No se pudo conectar a la base de datos")
		return nil, err
	}

	ctx, cancel := newContext()
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		log.Println("No se pudo conectar a la base de datos")
		return nil, err
	}

	err = client.Ping(context.Background(), readpref.Primary())
	if err != nil {
		log.Println("Error en el Ping a Mongo Server")
		return nil, err
	}

	return &Mongo{client}, nil
}

// crear contexto específico para cada operación
func newContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

// Create inserta un registro
func (m *Mongo) Create(document interface{}, collection string, database string) (primitive.ObjectID, error) {
	coll := m.Client.Database(database).Collection(collection)

	// Generamos un contexto específico para esta operación
	ctx, cancel := newContext()
	defer cancel()

	res, err := coll.InsertOne(ctx, document)
	if err != nil {
		return primitive.NilObjectID, err
	}
	return res.InsertedID.(primitive.ObjectID), err
}

// Update actualiza un registro
func (m *Mongo) Update(filter bson.M, update interface{}, collection string, database string) error {
	coll := m.Client.Database(database).Collection(collection)

	// Generamos un contexto específico para esta operación
	ctx, cancel := newContext()
	defer cancel()

	_, err := coll.UpdateOne(ctx, filter, bson.M{"$set": update})
	return err
}

// GetAndUpdate .
func (m *Mongo) GetAndUpdate(filter bson.M, update interface{}, collection, database string) error {
	coll := m.Client.Database(database).Collection(collection)

	// Generamos un contexto específico para esta operación
	ctx, cancel := newContext()
	defer cancel()

	// Recibe el documento anterior
	prev := bson.M{}

	// Soporte a push (Sólo bson.M)
	var bm bson.M
	switch update.(type) {
	case *bson.M:
		bm = *update.(*bson.M)
	case *map[string]interface{}:
		bm = bson.M(*update.(*map[string]interface{}))
	}

	var res *mongo.SingleResult
	if bm != nil {
		set := bson.M{}
		if bm["$push"] != nil {
			set["$addToSet"] = bm["$push"]
		}
		if bm["$inc"] != nil {
			set["$inc"] = bm["$inc"]
		}
		delete(bm, "$push")
		delete(bm, "$inc")
		if len(bm) > 0 {
			set["$set"] = bm
		}

		res = coll.FindOneAndUpdate(ctx, filter, set)
	} else {
		res = coll.FindOneAndUpdate(ctx, filter, bson.M{"$set": update})
	}
	err := res.Decode(&prev)
	if err != nil {
		return err
	}

	// unificamos (prev está completo)
	err = mapstructure.Decode(update, &prev)
	if err != nil {
		return err
	}

	// Actualizamos update (No sabemos el type)
	err = mapstructure.Decode(prev, update)
	if err != nil {
		return err
	}

	return nil
}

// GetAll recupera los registros, el output debe ser un *[]X
func (m *Mongo) GetAll(f *Filter, output interface{}, collection string, database string) error {
	coll := m.Client.Database(database).Collection(collection)

	// Generamos un contexto específico para esta operación
	ctx, cancel := newContext()
	defer cancel()

	// DDBB
	res, err := coll.Find(ctx, f.Filter, f.GetAll)
	if err != nil {
		return err
	}
	defer res.Close(ctx)

	return res.All(ctx, output)
}

// GetOne recupera un registro específico
func (m *Mongo) GetOne(filter bson.M, output interface{}, collection string, database string) error {
	coll := m.Client.Database(database).Collection(collection)

	// Generamos un contexto específico para esta operación
	ctx, cancel := newContext()
	defer cancel()

	// DDBB
	err := coll.FindOne(ctx, filter).Decode(output)
	return err
}

// Delete .
func (m *Mongo) Delete(filter bson.M, collection, database string) error {
	coll := m.Client.Database(database).Collection(collection)

	ctx, cancel := newContext()
	defer cancel()

	_, err := coll.DeleteOne(ctx, filter)
	return err
}

// Count ...
func (m *Mongo) Count(filter bson.M, collection, database string) (int64, error) {
	coll := m.Client.Database(database).Collection(collection)

	ctx, cancel := newContext()
	defer cancel()

	return coll.CountDocuments(ctx, filter)
}

// CreateIndex ...
func (m *Mongo) CreateIndex(keys bson.M, unique bool, collection, database string) error {
	coll := m.Client.Database(database).Collection(collection)

	index := mongo.IndexModel{
		Keys: keys,
		Options: &options.IndexOptions{
			Unique: &unique,
		},
	}

	ctx, cancel := newContext()
	defer cancel()

	_, err := coll.Indexes().CreateOne(ctx, index)
	return err
}
