package publisher

import (
	"encoding/csv"
	"github.com/ethereum/go-ethereum/statediff/builder"
	"os"
	"path/filepath"
	"strconv"
	"time"
	"github.com/ethereum/go-ethereum/common"
)

var (
	Headers = []string{
		"blockNumber", "blockHash", "accountAction", "codeHash",
		"nonceValue", "balanceValue", "contractRoot", "storageDiffPaths",
		"accountAddress", "storageKey", "storageValue",
	}

	timeStampFormat      = "20060102150405.00000"
	deletedAccountAction = "deleted"
	createdAccountAction = "created"
	updatedAccountAction = "updated"
)

func createCSVFilePath(path, blockNumber string) string {
	now := time.Now()
	timeStamp := now.Format(timeStampFormat)
	suffix := timeStamp + "-" + blockNumber
	filePath := filepath.Join(path, suffix)
	filePath = filePath + ".csv"
	return filePath
}

func (p *publisher) publishStateDiffToCSV(sd builder.StateDiff) (string, error) {
	filePath := createCSVFilePath(p.Config.Path, strconv.FormatInt(sd.BlockNumber, 10))

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return "", err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	var data [][]string
	data = append(data, Headers)
	for _, row := range accumulateAccountRows(sd, createdAccountAction) {
		data = append(data, row)
	}
	for _, row := range accumulateAccountRows(sd, updatedAccountAction) {
		data = append(data, row)
	}
	for _, row := range accumulateAccountRows(sd, deletedAccountAction) {
		data = append(data, row)
	}

	for _, value := range data {
		err := writer.Write(value)
		if err != nil {
			return "", err
		}
	}

	return filePath, nil
}

func accumulateAccountRows(sd builder.StateDiff, accountAction string) [][]string {
	var accountRows [][]string
	for accountAddr, accountDiff := range sd.UpdatedAccounts {
		formattedAccountData := formatAccountData(accountAddr, accountDiff, sd, accountAction)

		for _, accountData := range formattedAccountData {
			accountRows = append(accountRows, accountData)
		}
	}

	return accountRows
}

func formatAccountData(accountAddr common.Address, accountDiff builder.AccountDiff, sd builder.StateDiff, accountAction string) [][]string {
	blockNumberString := strconv.FormatInt(sd.BlockNumber, 10)
	blockHash := sd.BlockHash.String()
	codeHash := accountDiff.CodeHash
	nonce := strconv.FormatUint(*accountDiff.Nonce.Value, 10)
	balance := accountDiff.Balance.Value.String()
	newContractRoot := accountDiff.ContractRoot.Value
	address := accountAddr.String()
	var result [][]string

	for storagePath, storage := range accountDiff.Storage {
		formattedAccountData := []string{
			blockNumberString,
			blockHash,
			accountAction,
			codeHash,
			nonce,
			balance,
			*newContractRoot,
			storagePath,
			address,
			*storage.Key,
			*storage.Value,
		}

		result = append(result, formattedAccountData)
	}

	return result
}

