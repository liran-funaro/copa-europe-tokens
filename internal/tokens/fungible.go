// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package tokens

import (
	"encoding/json"
	"fmt"

	"github.com/copa-europe-tokens/internal/common"
	"github.com/copa-europe-tokens/pkg/constants"
	"github.com/copa-europe-tokens/pkg/types"
	"github.com/google/uuid"
	"github.com/pkg/errors"
)

const (
	reserveAccountUser = "reserve"
	mainAccount        = "main"
)

func FungibleTypeURL(typeId string) string {
	return common.URLForType(constants.FungibleTypeRoot, typeId)
}

type ReserveAccountComment struct {
	Supply uint64 `json:"supply"`
}

type ReserveAccountRecord struct {
	Balance uint64
	Supply  uint64
}

type FungibleTxContext struct {
	TokenTxContext
}

func getAccountKey(owner string, account string) string {
	return fmt.Sprintf("%s:%s", owner, account)
}

func getAccountKeyFromRecord(record *types.FungibleAccountRecord) string {
	return getAccountKey(record.Owner, record.Account)
}

func (ctx *FungibleTxContext) getReserveOwner() string {
	desc := ctx.getDescription()
	owner, ok := desc.Extension["reserveOwner"]
	if !ok {
		ctx.setError(common.NewErrInternal("reserve owner is not specified for token type [%s]", ctx.typeId))
	}
	return owner
}

func (ctx *FungibleTxContext) getAccountRecordRaw(owner string, account string) []byte {
	return ctx.Get(getAccountKey(owner, account))
}

func (ctx *FungibleTxContext) getAccountRecordOrDefault(
	owner string, account string, defaultAccount *types.FungibleAccountRecord,
) *types.FungibleAccountRecord {
	rawRecord := ctx.getAccountRecordRaw(owner, account)
	if rawRecord == nil {
		return defaultAccount
	}

	record := &types.FungibleAccountRecord{}
	err := json.Unmarshal(rawRecord, record)
	ctx.wrapError(err, "failed to json.Unmarshal %s", rawRecord)
	return record
}

func (ctx *FungibleTxContext) getAccountRecord(owner string, account string) *types.FungibleAccountRecord {
	defaultRecord := &types.FungibleAccountRecord{}
	record := ctx.getAccountRecordOrDefault(owner, account, defaultRecord)
	if record == defaultRecord {
		ctx.setError(common.NewErrNotFound("account [%v] of user [%s] does not exists", account, owner))
	}
	return record
}

func (ctx *FungibleTxContext) putAccountRecord(record *types.FungibleAccountRecord) {
	val, err := json.Marshal(record)
	ctx.wrapError(err, "failed to json.Marshal record")

	recordOwner := record.Owner
	if record.Owner == reserveAccountUser {
		recordOwner = ctx.getReserveOwner()
	}

	ctx.Put(getAccountKeyFromRecord(record), val, recordOwner, record.Account == mainAccount)
}

func (ctx *FungibleTxContext) deleteAccountRecord(record *types.FungibleAccountRecord) {
	ctx.Delete(getAccountKeyFromRecord(record))
}

func (ctx *FungibleTxContext) decodeReserveAccountComment(serializedComment string) *ReserveAccountComment {
	comment := &ReserveAccountComment{Supply: 0}
	if serializedComment == "" {
		return comment
	}
	err := json.Unmarshal([]byte(serializedComment), comment)
	ctx.wrapError(err, "failed to json.Unmarshal comment %s", serializedComment)
	return comment
}

func (ctx *FungibleTxContext) encodeReserveAccountComment(comment *ReserveAccountComment) string {
	byteComment, err := json.Marshal(comment)
	ctx.wrapError(err, "failed to json.Marshal comment")
	return string(byteComment)
}

func (ctx *FungibleTxContext) getReserveAccount() *ReserveAccountRecord {
	record := ctx.getAccountRecordOrDefault(reserveAccountUser, mainAccount, nil)
	if record == nil {
		return &ReserveAccountRecord{Balance: 0, Supply: 0}
	}
	comment := ctx.decodeReserveAccountComment(record.Comment)
	return &ReserveAccountRecord{
		Balance: record.Balance,
		Supply:  comment.Supply,
	}
}

func (ctx *FungibleTxContext) putReserveAccount(reserve *ReserveAccountRecord) {
	ctx.putAccountRecord(&types.FungibleAccountRecord{
		Account: mainAccount,
		Owner:   reserveAccountUser,
		Balance: reserve.Balance,
		Comment: ctx.encodeReserveAccountComment(&ReserveAccountComment{Supply: reserve.Supply}),
	})
}

func (ctx *FungibleTxContext) queryAccounts(owner string, account string) []types.FungibleAccountRecord {
	var query string
	if owner != "" && account != "" {
		query = fmt.Sprintf(`
		{
			"selector":
			{
				"$and": {
					"owner": {"$eq": "%s"},
					"account": {"$eq": "%s"}
				}
			}
		}`, owner, account)
	} else if owner != "" {
		query = fmt.Sprintf(`
		{
			"selector": {
				"owner": {"$eq": "%s"}
			}
		}`, owner)
	} else if account != "" {
		query = fmt.Sprintf(`
		{
			"selector": {
				"account": {"$eq": "%s"}
			}
		}`, account)
	} else {
		query = `
		{
			"selector": {
				"owner": {"$lte": "~"}
			}
		}`
	}

	jq, err := ctx.session.Query()
	ctx.wrapError(err, "failed to create JSONQuery")
	if jq == nil {
		return nil
	}

	queryResults, err := jq.ExecuteJSONQuery(ctx.getTokenDBName(), query)
	ctx.wrapOrionError(err, "failed to execute JSONQuery for token type [%s]", ctx.typeId)

	records := make([]types.FungibleAccountRecord, len(queryResults))
	for i, res := range queryResults {
		if err = json.Unmarshal(res.GetValue(), &records[i]); err != nil {
			ctx.setError(errors.Wrap(err, "failed to json.Unmarshal JSONQuery result"))
			break
		}
	}
	return records
}

func (ctx *FungibleTxContext) internalFungiblePrepareTransfer(request *types.FungibleTransferRequest) *types.FungibleTransferResponse {
	txUUID, err := uuid.NewRandom()
	ctx.wrapError(err, "Failed to generate tx ID")
	newRecord := types.FungibleAccountRecord{
		Account: txUUID.String(),
		Owner:   request.NewOwner,
		Balance: request.Quantity,
		Comment: request.Comment,
	}

	// Make sure we start a new TX to avoid false sharing with previous attempts
	if ctx.ResetDataTx(); ctx.haveError() {
		return nil
	}

	// Verify that the new generated account does not exist
	val := ctx.getAccountRecordRaw(newRecord.Owner, newRecord.Account)
	if val != nil {
		// Account already exist. We need to retry with a new TX
		// to avoid false sharing between the existing account.
		return nil // Signify the calling method to retry.
	}

	fromRecord := ctx.getAccountRecord(request.Owner, request.Account)
	if request.Quantity > fromRecord.Balance {
		ctx.setError(common.NewErrInvalid("Insufficient funds in account %v of %v. Requested %v, Balance: %v", fromRecord.Account, fromRecord.Owner, request.Quantity, fromRecord.Balance))
		return nil
	}

	fromRecord.Balance -= request.Quantity
	ctx.putAccountRecord(fromRecord)
	ctx.putAccountRecord(&newRecord)
	ctx.Prepare()
	return &types.FungibleTransferResponse{
		TypeId:        ctx.typeId,
		Owner:         request.Owner,
		Account:       request.Account,
		NewOwner:      newRecord.Owner,
		NewAccount:    newRecord.Account,
		TxEnvelope:    ctx.txEnvelope,
		TxPayloadHash: ctx.txPayloadHash,
	}
}
