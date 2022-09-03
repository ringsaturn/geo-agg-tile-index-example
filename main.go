package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
	"github.com/ringsaturn/xmongo"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	// MongoDB
	databaseName   = "foo"
	collectionName = "bar"

	// Zoom levles
	minZoom int = 0
	maxZoom int = 13
)

type GeoPoint struct {
	Type        string    `bson:"type" json:"type"`
	Coordinates []float64 `bson:"coordinates" json:"coordinates"`
}

type Tile struct {
	X, Y, Z    uint32
	Key        string
	orbmaptile *maptile.Tile
}

func (t *Tile) Center() [2]float64 {
	if t.orbmaptile == nil {
		_tmp := maptile.New(t.X, t.Y, maptile.Zoom(t.Z))
		t.orbmaptile = &_tmp
	}
	return t.orbmaptile.Center()
}

type Record struct {
	ID       primitive.ObjectID `bson:"_id"`                      // ObjectID
	Location GeoPoint           `bson:"location" json:"location"` // Raw point
	Levels   []Tile             `bson:"levels" json:"-"`          // Not export to outside in JSON
}

func (r *Record) SetLevels() {
	r.Levels = make([]Tile, 0)
	for z := minZoom; z <= maxZoom; z++ {
		orbmaptile := maptile.At(orb.Point{r.Location.Coordinates[0], r.Location.Coordinates[1]}, maptile.Zoom(z))
		x := orbmaptile.X
		y := orbmaptile.Y
		r.Levels = append(r.Levels, Tile{
			X:          x,
			Y:          y,
			Z:          uint32(z),
			Key:        fmt.Sprintf("%v-%v-%v", x, y, z),
			orbmaptile: &orbmaptile,
		})
	}
}

// https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/7ahn-ypff
//
// https://gist.githubusercontent.com/kashuk/670a350ea1f9fc543c3f6916ab392f62/raw/4c5ced45cc94d5b00e3699dd211ad7125ee6c4d3/NYC311_noise.csv
//
//go:embed NYC311_noise.csv
var exampleGeosCSV []byte

func SetupDemoData() []Record {
	lines := strings.Split(string(exampleGeosCSV), "\n")
	ret := make([]Record, 0)
	for index, line := range lines {
		if index == 0 {
			continue
		}
		rawparts := strings.Split(line, ",")
		if len(rawparts) != 2 {
			continue
		}
		lat_str := rawparts[0]
		long_str := rawparts[1]

		lat_float, err := strconv.ParseFloat(lat_str, 64)
		if err != nil {
			panic(err)
		}
		long_float, err := strconv.ParseFloat(long_str, 64)
		if err != nil {
			panic(err)
		}
		record := Record{
			ID:       primitive.NewObjectID(),
			Location: GeoPoint{Type: "Point", Coordinates: []float64{long_float, lat_float}},
		}
		record.SetLevels()
		ret = append(ret, record)
	}
	return ret
}

type RawStats struct {
	ID    string `bson:"_id"`
	Count int    `bson:"count"`
}

func FromRawStatsToGeoJSONFeatureItem(raw RawStats) GeoJSONFeatureItem {
	parts := strings.Split(raw.ID, "-")
	xStr, yStr, zStr := parts[0], parts[1], parts[2]
	x, _ := strconv.ParseInt(xStr, 10, 64)
	y, _ := strconv.ParseInt(yStr, 10, 64)
	z, _ := strconv.ParseInt(zStr, 10, 64)
	obrMapTile := maptile.New(uint32(x), uint32(y), maptile.Zoom(z))
	center := obrMapTile.Center()
	centerLng := center[0]
	centerLat := center[1]
	return GeoJSONFeatureItem{
		Type:       "Feature",
		Properties: map[string]interface{}{"count": raw.Count, "tileKey": raw.ID},
		Geometry: GeoPoint{
			Type:        "Point",
			Coordinates: []float64{centerLng, centerLat},
		},
	}
}

type GeoJSONFeatureItem struct {
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties"`
	Geometry   GeoPoint               `json:"geometry"`
}

type GeoJSONFeatures struct {
	Type     string               `json:"type"`
	Features []GeoJSONFeatureItem `json:"features"`
}

func demo(ctx context.Context, repo *xmongo.Repo[Record], level int) {
	pipes := bson.A{
		bson.M{
			"$match": bson.M{"levels.z": level},
		},
		bson.M{
			"$unwind": "$levels",
		},
		bson.M{
			"$match": bson.M{"levels.z": level},
		},
		bson.M{
			"$group": bson.M{
				"_id":   "$levels.key",
				"count": bson.M{"$sum": 1},
			},
		},
	}
	cursor, err := repo.Aggregate(ctx, pipes)
	if err != nil {
		log.Panicln("Aggregate err", err.Error())
	}
	rawRes, err := xmongo.Decode[RawStats](ctx, cursor)
	if err != nil {
		log.Panicln("Decode err", err.Error())
	}

	res := make([]GeoJSONFeatureItem, len(rawRes))
	for index, item := range rawRes {
		res[index] = FromRawStatsToGeoJSONFeatureItem(item)
	}
	finalRes := GeoJSONFeatures{
		Type:     "FeatureCollection",
		Features: res,
	}

	content, _ := json.MarshalIndent(finalRes, "", "  ")
	fmt.Println(string(content))
}

func main() {
	var needInsertData bool
	var level int
	flag.BoolVar(&needInsertData, "insert", false, "")
	flag.IntVar(&level, "level", 12, "level to run aggregate")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		panic(err)
	}

	err = client.Connect(ctx)
	if err != nil {
		panic(err)
	}
	collection := client.Database(databaseName).Collection(collectionName)
	repo, _ := xmongo.NewRepo[Record](collection)

	if needInsertData {
		demos := SetupDemoData()
		_, err := repo.InsertMany(ctx, demos)
		if err != nil {
			panic(err)
		}
	}

	demo(ctx, repo, level)
}
