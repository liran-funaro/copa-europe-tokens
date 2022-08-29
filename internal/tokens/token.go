// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package tokens

import (
	"encoding/json"

	"github.com/copa-europe-tokens/internal/common"
	"github.com/copa-europe-tokens/pkg/types"
	oriontypes "github.com/hyperledger-labs/orion-server/pkg/types"
	"github.com/pkg/errors"
)

// TokenTxContext handles a token transaction from start to finish
type TokenTxContext struct {
	txContext

	typeId string

	// Evaluated lazily
	tokenDBName string
	description *types.TokenDescription
}

func getTokenTypeDBName(typeId string) (string, error) {
	ret := TokenTypeDBNamePrefix + typeId
	if err := validateMD5Base64ID(typeId, "token type"); err != nil {
		return ret, common.NewErrInvalid("Invalid type ID: %s", err)
	}
	return ret, nil
}

func (ctx *TokenTxContext) getTokenDBName() string {
	if ctx.tokenDBName != "" {
		return ctx.tokenDBName
	}

	tokenDBName, err := getTokenTypeDBName(ctx.typeId)
	ctx.setError(err)
	ctx.tokenDBName = tokenDBName
	return tokenDBName
}

func (ctx *TokenTxContext) Get(key string) []byte {
	tx := ctx.dataTx()
	if ctx.haveError() || tx == nil {
		return nil
	}
	val, meta, err := tx.Get(ctx.getTokenDBName(), key)
	ctx.wrapOrionError(err, "failed to get ket [%v] from db [%s] with metadata [%+v]", key, ctx.getTokenDBName(), meta)
	return val
}

func (ctx *TokenTxContext) Put(key string, val []byte, owner string, mustSign bool) {
	tx := ctx.dataTx()
	if ctx.haveError() || tx == nil {
		return
	}
	err := tx.Put(ctx.getTokenDBName(), key, val, &oriontypes.AccessControl{
		ReadWriteUsers: map[string]bool{
			ctx.sessionUserId: true,
			owner:             true,
		},
		SignPolicyForWrite: oriontypes.AccessControl_ALL,
	})
	ctx.wrapOrionError(err, "failed to put [%s] in db [%s]", key, ctx.getTokenDBName())
	if mustSign {
		tx.AddMustSignUser(owner)
	}
}

func (ctx *TokenTxContext) Delete(key string) {
	tx := ctx.dataTx()
	if ctx.err != nil || tx == nil {
		return
	}
	err := tx.Delete(ctx.getTokenDBName(), key)
	ctx.wrapOrionError(err, "Fail to delete [%s] from db [%s]", key, ctx.getTokenDBName())
}

func (ctx *TokenTxContext) getDescription() (description *types.TokenDescription) {
	if ctx.description != nil {
		return ctx.description
	}

	description = &types.TokenDescription{}
	ctx.description = description

	// We create a new session since other users don't have read permissions to the types DB
	dataTx, err := ctx.session.DataTx()
	if err != nil {
		ctx.wrapError(err, "failed to create DataTx")
		return
	}
	defer abort(dataTx)

	val, meta, err := dataTx.Get(TypesDBName, ctx.getTokenDBName())
	ctx.wrapOrionError(err, "failed to Get description of type [%s]", ctx.typeId)
	if err != nil {
		return
	}
	if val == nil {
		ctx.setError(common.NewErrNotFound("token type not found"))
		return
	}

	err = json.Unmarshal(val, description)
	ctx.wrapError(err, "failed to json.Unmarshal %s for type %s", val, ctx.typeId)
	ctx.lg.Debugf("Token type description: %+v; metadata: %v", description, meta)
	return
}

func (ctx *TokenTxContext) createTokenDBTable(indices ...string) {
	dBsTx := ctx.dbsTx()
	if ctx.haveError() || dBsTx == nil {
		return
	}

	exists, err := dBsTx.Exists(ctx.getTokenDBName())
	ctx.wrapError(err, "failed to query DB existence")
	if exists {
		ctx.setError(common.NewErrExist("failed to deploy token: token type [%s] exists", ctx.typeId))
	}

	index := make(map[string]oriontypes.IndexAttributeType)
	for _, ind := range indices {
		index[ind] = oriontypes.IndexAttributeType_STRING
	}
	err = dBsTx.CreateDB(ctx.getTokenDBName(), index)
	ctx.wrapError(err, "failed to create DB")
	ctx.Commit()
}

func (ctx *TokenTxContext) publishTokenDescription() {
	if ctx.description == nil {
		ctx.setError(errors.New("No description was set in this transaction context"))
		return
	}

	// Save token description to Types-DB
	dataTx := ctx.dataTx()
	if ctx.haveError() || dataTx == nil {
		return
	}

	existingTokenDesc, _, err := dataTx.Get(TypesDBName, ctx.tokenDBName)
	ctx.wrapOrionError(err, "failed to get key [%s] from [%s]", ctx.tokenDBName, TypesDBName)
	if existingTokenDesc != nil {
		ctx.setError(errors.Errorf("failed to deploy token: custodian does not have privilege, but token type description exists: %s", string(existingTokenDesc)))
	}

	serializedDescription, err := json.Marshal(ctx.description)
	ctx.setError(err)
	err = dataTx.Put(TypesDBName, ctx.getTokenDBName(), serializedDescription, nil)
	ctx.wrapOrionError(err, "failed to put key [%s] to [%s]", ctx.getTokenDBName(), TypesDBName)
	ctx.Commit()
}
