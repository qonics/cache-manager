package model

import (
	"time"

	"github.com/gocql/gocql"
)

type LogsModel struct {
	Id        *gocql.UUID
	Activity  string     `json:"activity"`
	IpAddress string     `json:"id_address"`
	UserAgent string     `json:"user_agent"`
	UserId    gocql.UUID `json:"user_id"`
	CreatedAt time.Time  `json:"created_at"`
}
