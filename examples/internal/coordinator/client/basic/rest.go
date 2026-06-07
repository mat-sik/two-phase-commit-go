package basic

import (
	"time"
)

type PreparePayload struct {
	Payload   string    `json:"payload"`
	CreatedAt time.Time `json:"created_at"`
}
