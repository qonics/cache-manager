package model

import (
	"time"

	"github.com/gocql/gocql"
)

type CacheModel struct {
	Url       string    `json:"url" binding:"required"`
	Data      string    `json:"data" binding:"required"`
	Extra     string    `json:"extra"`
	Duration  uint      `json:"duration"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	PaymentId *gocql.UUID
}
