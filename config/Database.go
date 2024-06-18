package config

import (
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gocql/gocql"
	"github.com/jinzhu/gorm"
)

var DB *gorm.DB
var SESSION *gocql.Session

func ConnectDb() {

	cluster := gocql.NewCluster("127.0.0.1:9042")
	// Set up authentication
	cluster.Keyspace = os.Getenv("database")
	cluster.Consistency = gocql.One
	cluster.ConnectTimeout = time.Second * 5
	cluster.Timeout = time.Second * 5
	session, err := cluster.CreateSession()
	if err != nil {
		panic("Database connection error " + err.Error())
	}
	SESSION = session
	// connectionUrl := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8&parseTime=True&loc=Local", os.Getenv("user"), os.Getenv("password"), os.Getenv("host"), os.Getenv("port"), os.Getenv("database"))
	// fmt.Println(connectionUrl)
	// database, err := gorm.Open("mysql", connectionUrl)
	// fmt.Println(connectionUrl)
	// if err != nil {
	// 	panic("Database connection error " + err.Error())
	// }
	// database.AutoMigrate(&model.AssessmentDistribution{})
}
