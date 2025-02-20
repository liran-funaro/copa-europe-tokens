// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

package tokens

import (
	"encoding/json"
	"fmt"

	"github.com/copa-europe-tokens/internal/common"
	"github.com/copa-europe-tokens/pkg/constants"
	"github.com/copa-europe-tokens/pkg/types"
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

func newFungibleTxContext(m *Manager, typeId string) (*FungibleTxContext, error) {
	genericCtx, err := newTokenTxContext(m, typeId)
	if err != nil {
		return nil, err
	}
	return &FungibleTxContext{*genericCtx}, nil
}

func getAccountKey(owner string, account string) string {
	return fmt.Sprintf("%s:%s", owner, account)
}

func getAccountKeyFromRecord(record *types.FungibleAccountRecord) string {
	return getAccountKey(record.Owner, record.Account)
}

func (ctx *FungibleTxContext) getReserveOwner() (string, error) {
	desc, err := ctx.getDescription()
	if err != nil {
		return "", err
	}
	owner, ok := desc.Extension["reserveOwner"]
	if !ok {
		return "", common.NewErrInternal("Reserve owner is not specified for token type [%s]", ctx.typeId)
	}
	return owner, nil
}

func (ctx *FungibleTxContext) getAccountRecordRaw(owner string, account string) ([]byte, error) {
	return ctx.Get(getAccountKey(owner, account))
}

func unmarshalAccountRecord(rawRecord []byte) (*types.FungibleAccountRecord, error) {
	record := types.FungibleAccountRecord{}
	if err := json.Unmarshal(rawRecord, &record); err != nil {
		return nil, errors.Wrapf(err, "Failed to json.Unmarshal %s", rawRecord)
	}
	return &record, nil
}

func (ctx *FungibleTxContext) getAccountRecord(owner string, account string) (*types.FungibleAccountRecord, error) {
	val, err := ctx.getAccountRecordRaw(owner, account)
	if err != nil {
		return nil, err
	}
	if val == nil {
		ctx.lg.Debugf("Account does not exists: DB: %s, user %s, account: %s", ctx.tokenDBName, owner, account)
		return nil, common.NewErrNotFound("Account does not exists [%v:%v]", owner, account)
	}

	return unmarshalAccountRecord(val)
}

func (ctx *FungibleTxContext) putAccountRecord(record *types.FungibleAccountRecord) error {
	val, err := json.Marshal(record)
	if err != nil {
		return errors.Wrap(err, "failed to json.Marshal record")
	}

	recordOwner := record.Owner
	if record.Owner == reserveAccountUser {
		recordOwner, err = ctx.getReserveOwner()
		if err != nil {
			return err
		}
	}

	return ctx.Put(getAccountKeyFromRecord(record), val, recordOwner, record.Account == mainAccount)
}

func (ctx *FungibleTxContext) deleteAccountRecord(record *types.FungibleAccountRecord) error {
	return ctx.Delete(getAccountKeyFromRecord(record))
}

func decodeReserveAccountComment(serializedComment string) (*ReserveAccountComment, error) {
	comment := ReserveAccountComment{Supply: 0}
	if serializedComment == "" {
		return &comment, nil
	}
	if err := json.Unmarshal([]byte(serializedComment), &comment); err != nil {
		return nil, errors.Wrapf(err, "failed to json.Unmarshal comment %s", serializedComment)
	}
	return &comment, nil
}

func encodeReserveAccountComment(comment *ReserveAccountComment) (string, error) {
	byteComment, err := json.Marshal(comment)
	if err != nil {
		return "", errors.Wrap(err, "failed to json.Marshal comment")
	}
	return string(byteComment), nil
}

func (ctx *FungibleTxContext) getReserveAccount() (*ReserveAccountRecord, error) {
	rawRecord, err := ctx.getAccountRecordRaw(reserveAccountUser, mainAccount)
	if err != nil {
		return nil, err
	}
	if rawRecord == nil {
		return &ReserveAccountRecord{Balance: 0, Supply: 0}, nil
	}

	record, err := unmarshalAccountRecord(rawRecord)
	if err != nil {
		return nil, err
	}
	comment, err := decodeReserveAccountComment(record.Comment)
	if err != nil {
		return nil, err
	}

	return &ReserveAccountRecord{
		Balance: record.Balance,
		Supply:  comment.Supply,
	}, nil
}

func (ctx *FungibleTxContext) putReserveAccount(reserve *ReserveAccountRecord) error {
	serializedComment, err := encodeReserveAccountComment(&ReserveAccountComment{Supply: reserve.Supply})
	if err != nil {
		return err
	}
	return ctx.putAccountRecord(&types.FungibleAccountRecord{
		Account: mainAccount,
		Owner:   reserveAccountUser,
		Balance: reserve.Balance,
		Comment: serializedComment,
	})
}

func (ctx *FungibleTxContext) queryAccounts(owner string, account string) ([]types.FungibleAccountRecord, error) {
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

	jq, err := ctx.custodianSession.Query()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create JSONQuery")
	}
	queryResults, err := jq.ExecuteJSONQuery(ctx.tokenDBName, query)
	if err != nil {
		return nil, wrapOrionError(err, "Failed to execute JSONQuery for token type [%s]", ctx.typeId)
	}

	records := make([]types.FungibleAccountRecord, len(queryResults))
	for i, res := range queryResults {
		if err = json.Unmarshal(res.GetValue(), &records[i]); err != nil {
			return nil, errors.Wrap(err, "Failed to json.Unmarshal JSONQuery result")
		}
	}
	return records, nil
}
