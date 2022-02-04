package apis

import (
	"encoding/base64"
	"github/cmdit-veasna/sample-go/internal/data"
	"net/http"

	"github.com/gin-gonic/gin"
	uuid "github.com/satori/go.uuid"
)

func SampleInputHandler(ctx *gin.Context) {
	pool, err := data.GetPool(ctx)
	if err != nil {
		ctx.AbortWithError(http.StatusInternalServerError, err)
		return
	}

	defer ctx.Request.Body.Close()
	id := base64.RawURLEncoding.EncodeToString(uuid.NewV4().Bytes())
	err = <-pool.RecieverJob(ctx, id, ctx.Request.Body)
	// inspect header of content type if required
	if err != nil {
		ctx.AbortWithError(http.StatusInternalServerError, err)
	} else {
		ctx.Status(http.StatusOK)
	}
}
