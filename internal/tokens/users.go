package tokens

import (
	"github.com/copa-europe-tokens/internal/common"
	oriontypes "github.com/hyperledger-labs/orion-server/pkg/types"
)

// UserTxContext handles a user transaction from start to finish
type UserTxContext struct {
	txContext

	userId string

	// Evaluated lazily
	record *oriontypes.User
}

func (ctx *UserTxContext) Get() *oriontypes.User {
	if ctx.record != nil {
		return ctx.record
	}

	tx := ctx.usersTx()
	if ctx.haveError() || tx == nil {
		return nil
	}

	record, err := tx.GetUser(ctx.userId)
	ctx.wrapOrionError(err, "failed to get user [%s]", ctx.userId)
	ctx.record = record
	return record
}

func (ctx *UserTxContext) ValidateUserExists() *oriontypes.User {
	record := ctx.Get()
	if record == nil {
		ctx.setError(common.NewErrNotFound("user not found: %s", ctx.userId))
	}
	return record
}

func (ctx *UserTxContext) Put(record *oriontypes.User) {
	tx := ctx.usersTx()
	if ctx.haveError() || tx == nil {
		return
	}
	err := tx.PutUser(record, nil)
	ctx.wrapError(err, "failed to put user: %s", record.Id)
	ctx.record = record
}

func (ctx *UserTxContext) AddPrivilege(typeIds ...string) {
	if len(typeIds) == 0 {
		return
	}

	record := ctx.ValidateUserExists()
	if ctx.haveError() {
		return
	}

	dbPerm := record.Privilege.DbPermission
	if dbPerm == nil {
		dbPerm = make(map[string]oriontypes.Privilege_Access)
		record.Privilege.DbPermission = dbPerm
	}

	for _, typeId := range typeIds {
		dbName, err := getTokenTypeDBName(typeId)
		ctx.setError(err)
		dbPerm[dbName] = oriontypes.Privilege_ReadWrite
	}

	ctx.Put(record)
}
