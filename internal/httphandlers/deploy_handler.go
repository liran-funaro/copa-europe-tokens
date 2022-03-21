package httphandlers

import (
	"github.com/copa-europe-tokens/internal/tokens"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	"net/http"
)

type deployHandler struct {
	manager *tokens.Manager
	lg      *logger.SugarLogger
}

func NewDeployHandler(manager *tokens.Manager, lg *logger.SugarLogger) *deployHandler {
	return &deployHandler{
		manager: manager,
		lg:      lg}
}

func (d *deployHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	//TODO
}
