package mgodb

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

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
type Filter struct {
	Filter bson.M
	GetAll *options.FindOptions
}

var (
	session *Mongo
	once    sync.Once
)

// Get obtiene el cliente
func Get() *Mongo {
	return session
}

// GetFiltersOptions separa los filtros y parametros desde la URL
func GetFiltersOptions(r *http.Request, format map[string]string) (*Filter, error) {
	keys := r.URL.Query()
	filters := bson.M{}
	parameters := &options.FindOptions{}

	// Recorremos los parametros para separarlos
	for key, value := range keys {
		switch key {
		case "sort":
			sort := bson.M{}
			ks := strings.Split(value[0], ",")
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
func Configure(username, password, host, port, authDB string) {
	once.Do(func() {
		var err error
		sess, err := newMongoClient(username, password, host, port, authDB)
		if err != nil {
			log.Println("Problema con mongo")
			log.Fatal(err)
		}
		log.Println("Instanciamos mongo")
		session = sess
	})
}

// newMongoClient Crea un cliente (se conecta a mongo)
func newMongoClient(username, password, host, port, authDB string) (*Mongo, error) {
	uri := "mongodb://"
	if username != "" || password != "" {
		uri += fmt.Sprintf("%s:%s@", username, password)
	}
	uri += fmt.Sprintf("%s:%s/", host, port)
	if authDB != "" {
		uri += authDB
	}
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
