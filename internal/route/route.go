package route

import (
	"github/cmdit-veasna/sample-go/internal/apis"

	"github.com/gin-gonic/gin"
)

// GetRoute return a `gin.Engine` that implement `http.Handler`.
// All apis is registers within the returned instance.
func GetRoute() *gin.Engine {
	router := gin.Default()

	// more apis path can register here
	router.POST("/sample", apis.SampleInputHandler)

	return router
}
