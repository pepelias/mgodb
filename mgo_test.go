package mgodb

import (
	"testing"

	"github.com/kr/pretty"
	"gopkg.in/mgo.v2/bson"
)

func TestGetJoined(t *testing.T) {
	Configure(
		"",
		"",
		"localhost",
		"27017",
		"admin",
		false,
	)
	db := Get()
	user := []bson.M{}
	err := db.GetJoined(
		bson.M{"email": "fullpepelias@gmail.com"},
		Lookup{
			From:         "commerces",
			LocalField:   "commerce_id",
			ForeignField: "_id",
			As:           "commerce",
		},
		true,
		&user,
		"users",
		"mercadotienda",
	)
	if err != nil {
		t.Logf("Error: Algo fallo %s", err.Error())
		t.Fail()
	}
	pretty.Println(user)
}
