// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package statediff_test

import (
	"math/big"
	"reflect"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/statediff"
	"github.com/ethereum/go-ethereum/statediff/testhelpers/mocks"
)

func TestServiceLoop(t *testing.T) {
	testWhenThereAreNoStateDiffs(t)
	testWhenThereAreStateDiffs(t)
	testSomeOfTheStateDiffsAreEmpty(t)
	testWatchedAddresses(t)
}

var (
	stateChangeEventCh = make(chan core.StateChangeEvent, 1)

	testBlock1 = types.NewBlock(&types.Header{}, nil, nil, nil)
	testBlock2 = types.NewBlock(&types.Header{}, nil, nil, nil)
	testBlock3 = types.NewBlock(&types.Header{}, nil, nil, nil)

	testAccount1Address = common.HexToAddress("0x1")
	testAccount2Address = common.HexToAddress("0x2")
	testAccount3Address = common.HexToAddress("0x3")

	modifiedAccount1 = state.ModifiedAccount{
		Account: state.Account{
			Nonce:   0,
			Balance: big.NewInt(100),
			Root:    common.HexToHash("0x01"),
		},
		Storage: nil,
	}

	account2StorageKey   = common.HexToHash("0x0002")
	account2StorageValue = common.HexToHash("0x00002")
	modifiedAccount2     = state.ModifiedAccount{
		Account: state.Account{
			Nonce:   0,
			Balance: big.NewInt(200),
			Root:    common.HexToHash("0x02"),
		},
		Storage: state.Storage{account2StorageKey: account2StorageValue},
	}

	modifiedAccount3 = state.ModifiedAccount{
		Account: state.Account{
			Nonce:   0,
			Balance: big.NewInt(300),
			Root:    common.HexToHash("0x03"),
		},
		Storage: nil,
	}

	event1 = core.StateChangeEvent{
		Block: testBlock1,
		StateChanges: state.StateChanges{
			testAccount1Address: modifiedAccount1,
			testAccount2Address: modifiedAccount2,
		},
	}

	event2 = core.StateChangeEvent{
		Block: testBlock2,
		StateChanges: state.StateChanges{
			testAccount3Address: modifiedAccount3,
		},
	}

	// The mock Blockchain sends an error for any event after the second on
	erroredStateChangeEvent = core.StateChangeEvent{Block: testBlock3, StateChanges: state.StateChanges{}}
)

func testWhenThereAreNoStateDiffs(t *testing.T) {
	blockChain := mocks.BlockChain{}
	service := statediff.Service{
		Mutex:            sync.Mutex{},
		BlockChain:       &blockChain,
		QuitChan:         make(chan bool),
		Subscriptions:    make(map[rpc.ID]statediff.Subscription),
		WatchedAddresses: []string{}, // when empty, return all diffs
	}
	payloadChan := make(chan statediff.Payload, 2)
	quitChan := make(chan bool)
	service.Subscribe(rpc.NewID(), payloadChan, quitChan)
	noStateChangeEventBlock1 := core.StateChangeEvent{Block: testBlock1, StateChanges: state.StateChanges{}}
	blockChain.SetStateChangeEvents([]core.StateChangeEvent{noStateChangeEventBlock1, erroredStateChangeEvent})

	payloads := make([]statediff.Payload, 0, 2)
	wg := sync.WaitGroup{}
	go func() {
		wg.Add(1)
		for i := 0; i < 1; i++ {
			select {
			case payload := <-payloadChan:
				payloads = append(payloads, payload)
			case <-quitChan:
			}
		}
		wg.Done()
	}()

	service.Loop(stateChangeEventCh)
	wg.Wait()
	if len(payloads) != 0 {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual number of payloads does not equal expected.\nactual: %+v\nexpected: 2", len(payloads))
	}
	expectedPayloads := []statediff.Payload{}

	// If there are no statediffs the payloads include empty statediffs
	if !reflect.DeepEqual(payloads, expectedPayloads) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual payload equal expected.\nactual:%+v\nexpected: %+v", payloads, expectedPayloads)
	}
}

func testWhenThereAreStateDiffs(t *testing.T) {
	blockChain := mocks.BlockChain{}
	service := statediff.Service{
		Mutex:            sync.Mutex{},
		BlockChain:       &blockChain,
		QuitChan:         make(chan bool),
		Subscriptions:    make(map[rpc.ID]statediff.Subscription),
		WatchedAddresses: []string{}, // when empty, return all diffs
	}
	payloadChan := make(chan statediff.Payload, 2)
	quitChan := make(chan bool)
	service.Subscribe(rpc.NewID(), payloadChan, quitChan)
	blockChain.SetStateChangeEvents([]core.StateChangeEvent{event1, event2, erroredStateChangeEvent})

	payloads := make([]statediff.Payload, 0, 2)
	wg := sync.WaitGroup{}
	go func() {
		wg.Add(1)
		for i := 0; i < 2; i++ {
			select {
			case payload := <-payloadChan:
				payloads = append(payloads, payload)
			case <-quitChan:
			}
		}
		wg.Done()
	}()

	service.Loop(stateChangeEventCh)
	wg.Wait()
	if len(payloads) != 2 {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual number of payloads does not equal expected.\nactual: %+v\nexpected: 2", len(payloads))
	}

	stateDiffFromEvent1 := statediff.StateDiff{
		BlockNumber: testBlock1.Number(),
		BlockHash:   testBlock1.Hash(),
		UpdatedAccounts: []statediff.AccountDiff{
			getAccountDiff(testAccount1Address, modifiedAccount1, t),
			getAccountDiff(testAccount2Address, modifiedAccount2, t),
		},
	}
	expectedStateDiffRlpFromEvent1, err := rlp.EncodeToBytes(stateDiffFromEvent1)
	if err != nil {
		t.Error("Test failure:", t.Name())
		t.Logf("Failed to encode state diff to bytes")
	}

	stateDiffFromEvent2 := statediff.StateDiff{
		BlockNumber: testBlock2.Number(),
		BlockHash:   testBlock2.Hash(),
		UpdatedAccounts: []statediff.AccountDiff{
			getAccountDiff(testAccount3Address, modifiedAccount3, t),
		},
	}
	expectedStateDiffRlpFromEvent2, err := rlp.EncodeToBytes(stateDiffFromEvent2)
	if err != nil {
		t.Error("Test failure:", t.Name())
		t.Logf("Failed to encode state diff to bytes")
	}

	expectedPayloads := []statediff.Payload{
		{StateDiffRlp: expectedStateDiffRlpFromEvent1},
		{StateDiffRlp: expectedStateDiffRlpFromEvent2},
	}
	if !reflect.DeepEqual(payloads, expectedPayloads) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual payload equal expected.\nactual:%+v\nexpected: %+v", payloads, expectedPayloads)
	}
}

func testSomeOfTheStateDiffsAreEmpty(t *testing.T) {
	blockChain := mocks.BlockChain{}
	service := statediff.Service{
		Mutex:            sync.Mutex{},
		BlockChain:       &blockChain,
		QuitChan:         make(chan bool),
		Subscriptions:    make(map[rpc.ID]statediff.Subscription),
		WatchedAddresses: []string{}, // when empty, return all diffs
	}
	payloadChan := make(chan statediff.Payload, 2)
	quitChan := make(chan bool)
	service.Subscribe(rpc.NewID(), payloadChan, quitChan)
	noStateChangeEvent := core.StateChangeEvent{Block: testBlock2, StateChanges: state.StateChanges{}}
	blockChain.SetStateChangeEvents([]core.StateChangeEvent{event1, noStateChangeEvent, erroredStateChangeEvent})

	payloads := make([]statediff.Payload, 0, 2)
	wg := sync.WaitGroup{}
	go func() {
		wg.Add(1)
		for i := 0; i < 2; i++ {
			select {
			case payload := <-payloadChan:
				payloads = append(payloads, payload)
			case <-quitChan:
			}
		}
		wg.Done()
	}()

	service.Loop(stateChangeEventCh)
	wg.Wait()
	if len(payloads) != 1 {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual number of payloads does not equal expected.\nactual: %+v\nexpected: 2", len(payloads))
	}

	stateDiffFromEvent1 := statediff.StateDiff{
		BlockNumber: testBlock1.Number(),
		BlockHash:   testBlock1.Hash(),
		UpdatedAccounts: []statediff.AccountDiff{
			getAccountDiff(testAccount1Address, modifiedAccount1, t),
			getAccountDiff(testAccount2Address, modifiedAccount2, t),
		},
	}
	expectedStateDiffRlpFromEvent1, err := rlp.EncodeToBytes(stateDiffFromEvent1)
	if err != nil {
		t.Error("Test failure:", t.Name())
		t.Logf("Failed to encode state diff to bytes")
	}

	expectedPayloads := []statediff.Payload{
		{StateDiffRlp: expectedStateDiffRlpFromEvent1},
	}

	if !reflect.DeepEqual(payloads, expectedPayloads) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual payload equal expected.\nactual:%+v\nexpected: %+v", payloads, expectedPayloads)
	}
}
func testWatchedAddresses(t *testing.T) {
	blockChain := mocks.BlockChain{}
	service := statediff.Service{
		Mutex:            sync.Mutex{},
		BlockChain:       &blockChain,
		QuitChan:         make(chan bool),
		Subscriptions:    make(map[rpc.ID]statediff.Subscription),
		WatchedAddresses: []string{testAccount2Address.String()},
	}
	payloadChan := make(chan statediff.Payload, 2)
	quitChan := make(chan bool)
	service.Subscribe(rpc.NewID(), payloadChan, quitChan)
	blockChain.SetStateChangeEvents([]core.StateChangeEvent{event1, event2, erroredStateChangeEvent})

	payloads := make([]statediff.Payload, 0, 2)
	wg := sync.WaitGroup{}
	go func() {
		wg.Add(1)
		for i := 0; i < 2; i++ {
			select {
			case payload := <-payloadChan:
				payloads = append(payloads, payload)
			case <-quitChan:
			}
		}
		wg.Done()
	}()

	service.Loop(stateChangeEventCh)
	wg.Wait()
	if len(payloads) != 1 {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual number of payloads does not equal expected.\nactual: %+v\nexpected: 1", len(payloads))
	}

	stateDiff := statediff.StateDiff{
		BlockNumber:     testBlock1.Number(),
		BlockHash:       testBlock1.Hash(),
		UpdatedAccounts: []statediff.AccountDiff{getAccountDiff(testAccount2Address, modifiedAccount2, t)},
	}
	expectedStateDiffRlp, err := rlp.EncodeToBytes(stateDiff)
	if err != nil {
		t.Error("Test failure:", t.Name())
		t.Logf("Failed to encode state diff to bytes")
	}
	expectedPayloads := []statediff.Payload{
		{StateDiffRlp: expectedStateDiffRlp},
	}
	if !reflect.DeepEqual(payloads, expectedPayloads) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual payload equal expected.\nactual:%+v\nexpected: %+v", payloads, expectedPayloads)
	}
}

func getAccountDiff(accountAddress common.Address, modifedAccount state.ModifiedAccount, t *testing.T) statediff.AccountDiff {
	accountRlp, accountRlpErr := rlp.EncodeToBytes(modifedAccount.Account)
	if accountRlpErr != nil {
		t.Error("Test failure:", t.Name())
		t.Logf("Failed to encode account diff")
	}

	var storageDiffs []statediff.StorageDiff
	for key, value := range modifedAccount.Storage {
		storageValueRlp, storageRlpErr := rlp.EncodeToBytes(value)
		if storageRlpErr != nil {
			t.Error("Test failure:", t.Name())
			t.Logf("Failed to encode storgae diff")
		}
		storageDiff := statediff.StorageDiff{
			Key:   key[:],
			Value: storageValueRlp,
		}

		storageDiffs = append(storageDiffs, storageDiff)
	}

	return statediff.AccountDiff{
		Key:     accountAddress[:],
		Value:   accountRlp,
		Storage: storageDiffs,
	}
}
