// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package tokens

import (
	"encoding/base64"
	"encoding/json"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger-labs/orion-sdk-go/pkg/bcdb"
	"github.com/hyperledger-labs/orion-server/pkg/logger"
	oriontypes "github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

// txContext handles a transaction from start to finish
type txContext struct {
	lg            *logger.SugarLogger
	sessionUserId string
	session       bcdb.DBSession

	// Evaluated lazily
	curTx         bcdb.TxContext
	err           error
	txID          string
	receiptEnv    *oriontypes.TxReceiptResponseEnvelope
	txEnvelope    string
	txPayloadHash string
}

func (ctx *txContext) Error() error {
	return ctx.err
}

func (ctx *txContext) haveError() bool {
	return ctx.err != nil
}

func (ctx *txContext) setError(err error) {
	if err != nil && !ctx.haveError() {
		ctx.err = err
	}
}

func (ctx *txContext) wrapError(err error, format string, args ...interface{}) {
	if err != nil && !ctx.haveError() {
		ctx.err = errors.Wrapf(err, format, args...)
	}
}

func (ctx *txContext) wrapOrionError(err error, format string, args ...interface{}) {
	if err != nil && !ctx.haveError() {
		ctx.err = wrapOrionError(err, format, args...)
	}
}

// ResetTx creates a new transaction. It will abort previous transaction if existed.
func (ctx *txContext) ResetTx(newTx func() (bcdb.TxContext, error)) {
	if ctx.curTx != nil {
		ctx.Abort()
	}

	if ctx.haveError() {
		return
	}

	tx, err := newTx()
	ctx.wrapError(err, "failed to create TX")
	ctx.curTx = tx
}

func (ctx *txContext) newDataTx() (bcdb.TxContext, error) {
	return ctx.session.DataTx()
}

func (ctx *txContext) newUsersTx() (bcdb.TxContext, error) {
	return ctx.session.UsersTx()
}

func (ctx *txContext) newDBsTx() (bcdb.TxContext, error) {
	return ctx.session.DBsTx()
}

func (ctx *txContext) ResetDataTx() {
	ctx.ResetTx(ctx.newDataTx)
}

func (ctx *txContext) ResetUsersTx() {
	ctx.ResetTx(ctx.newUsersTx)
}

func (ctx *txContext) ResetDBsTx() {
	ctx.ResetTx(ctx.newDBsTx)
}

// getDataTx returns an existing "data" transaction, if exists
func (ctx *txContext) getDataTx() bcdb.DataTxContext {
	dataTx, _ := ctx.curTx.(bcdb.DataTxContext)
	return dataTx
}

// getUsersTx returns an existing "users" transaction, if exists
func (ctx *txContext) getUsersTx() bcdb.UsersTxContext {
	usersTx, _ := ctx.curTx.(bcdb.UsersTxContext)
	return usersTx
}

// getDBsTx returns an existing "DBs" transaction, if exists
func (ctx *txContext) getDBsTx() bcdb.DBsTxContext {
	dbTx, _ := ctx.curTx.(bcdb.DBsTxContext)
	return dbTx
}

// dataTx returns an existing "data" transaction or creates a new one
func (ctx *txContext) dataTx() bcdb.DataTxContext {
	if dataTx := ctx.getDataTx(); dataTx != nil {
		return dataTx
	}
	ctx.ResetDataTx()
	return ctx.getDataTx()
}

// usersTx returns an existing "users" transaction or creates a new one
func (ctx *txContext) usersTx() bcdb.UsersTxContext {
	if usersTx := ctx.getUsersTx(); usersTx != nil {
		return usersTx
	}
	ctx.ResetUsersTx()
	return ctx.getUsersTx()
}

// dbsTx returns an existing "DBs" transaction or creates a new one
func (ctx *txContext) dbsTx() bcdb.DBsTxContext {
	if dbTx := ctx.getDBsTx(); dbTx != nil {
		return dbTx
	}
	ctx.ResetDBsTx()
	return ctx.getDBsTx()
}

// Abort a TX if it was initiated
func (ctx *txContext) Abort() {
	if ctx == nil {
		return
	}

	if ctx.curTx == nil {
		return
	}

	if err := ctx.curTx.Abort(); err != bcdb.ErrTxSpent {
		ctx.wrapError(err, "failed to abort TX")
	}
	ctx.curTx = nil
}

func (ctx *txContext) Commit() {
	if ctx.haveError() {
		return
	}

	if ctx.curTx == nil {
		ctx.setError(errors.New("Attempt to commit a transaction, but transaction was not created."))
		return
	}

	txID, receiptEnv, err := ctx.curTx.Commit(true)
	ctx.wrapOrionError(err, "failed to commit")
	ctx.txID = txID
	ctx.receiptEnv = receiptEnv
}

func (ctx *txContext) Prepare() {
	if ctx.haveError() {
		return
	}

	dataTx := ctx.getDataTx()
	if dataTx == nil {
		ctx.setError(errors.New("Attempt to prepare a data transaction, but a data transaction was not created."))
		return
	}

	txEnv, err := dataTx.SignConstructedTxEnvelopeAndCloseTx()
	if err != nil {
		ctx.setError(errors.Wrap(err, "failed to construct Tx envelope"))
		return
	}

	txEnvBytes, err := proto.Marshal(txEnv)
	if err != nil {
		ctx.setError(errors.Wrap(err, "failed to proto.Marshal Tx envelope"))
		return
	}

	payloadBytes, err := json.Marshal(txEnv.(*oriontypes.DataTxEnvelope).Payload)
	if err != nil {
		ctx.setError(errors.Wrap(err, "failed to json.Marshal DataTx"))
		return
	}

	payloadHash, err := ComputeSHA256Hash(payloadBytes)
	if err != nil {
		ctx.setError(errors.Wrap(err, "failed to compute hash of DataTx bytes"))
		return
	}

	ctx.txEnvelope = base64.StdEncoding.EncodeToString(txEnvBytes)
	ctx.txPayloadHash = base64.StdEncoding.EncodeToString(payloadHash)
}
