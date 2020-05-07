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
	"bytes"
	"fmt"
	"math/big"
	"math/rand"
	"reflect"
	"sync"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/statediff"
	"github.com/ethereum/go-ethereum/statediff/testhelpers/mocks"
)

func TestServiceLoop(t *testing.T) {
	//testErrorInChainEventLoop(t)
	//testErrorInBlockLoop(t)
	testWhenAStateDiffIsEmpty(t)
}

var (
	eventsChannel = make(chan core.ChainEvent, 1)

	parentRoot1   = common.HexToHash("0x01")
	parentRoot2   = common.HexToHash("0x02")
	parentHeader1 = types.Header{Number: big.NewInt(rand.Int63()), Root: parentRoot1}
	parentHeader2 = types.Header{Number: big.NewInt(rand.Int63()), Root: parentRoot2}

	parentBlock1 = types.NewBlock(&parentHeader1, nil, nil, nil)
	parentBlock2 = types.NewBlock(&parentHeader2, nil, nil, nil)

	parentHash1 = parentBlock1.Hash()
	parentHash2 = parentBlock2.Hash()

	testRoot1 = common.HexToHash("0x03")
	testRoot2 = common.HexToHash("0x04")
	testRoot3 = common.HexToHash("0x05")
	testRoot4 = common.HexToHash("0x06")
	header1   = types.Header{Number: big.NewInt(rand.Int63()), ParentHash: parentHash1, Root: testRoot1}
	header2   = types.Header{Number: big.NewInt(rand.Int63()), ParentHash: parentHash2, Root: testRoot2}
	header3   = types.Header{Number: big.NewInt(rand.Int63()), ParentHash: common.HexToHash("parent hash"), Root: testRoot3}
	header4   = types.Header{Number: big.NewInt(rand.Int63()), ParentHash: common.HexToHash("parent hash"), Root: testRoot4}

	testBlock1 = types.NewBlock(&header1, nil, nil, nil)
	testBlock2 = types.NewBlock(&header2, nil, nil, nil)
	testBlock3 = types.NewBlock(&header3, nil, nil, nil)
	testBlock4 = types.NewBlock(&header4, nil, nil, nil)

	receiptRoot1  = common.HexToHash("0x07")
	receiptRoot2  = common.HexToHash("0x08")
	receiptRoot3  = common.HexToHash("0x09")
	testReceipts1 = []*types.Receipt{types.NewReceipt(receiptRoot1.Bytes(), false, 1000), types.NewReceipt(receiptRoot2.Bytes(), false, 2000)}
	testReceipts2 = []*types.Receipt{types.NewReceipt(receiptRoot3.Bytes(), false, 3000)}

	event1 = core.ChainEvent{Block: testBlock1}
	event2 = core.ChainEvent{Block: testBlock2}
	event3 = core.ChainEvent{Block: testBlock3}
	event4 = core.ChainEvent{Block: testBlock4}
)

func testErrorInChainEventLoop(t *testing.T) {
	//the third chain event causes and error (in blockchain mock)
	builder := mocks.Builder{}
	blockChain := mocks.BlockChain{}
	service := statediff.Service{
		Mutex:         sync.Mutex{},
		Builder:       &builder,
		BlockChain:    &blockChain,
		QuitChan:      make(chan bool),
		Subscriptions: make(map[rpc.ID]statediff.Subscription),
		StreamBlock:   true,
	}
	payloadChan := make(chan statediff.Payload, 2)
	quitChan := make(chan bool)
	service.Subscribe(rpc.NewID(), payloadChan, quitChan)
	testRoot2 = common.HexToHash("0xTestRoot2")
	blockMapping := make(map[common.Hash]*types.Block)
	blockMapping[parentBlock1.Hash()] = parentBlock1
	blockMapping[parentBlock2.Hash()] = parentBlock2
	blockChain.SetParentBlocksToReturn(blockMapping)
	blockChain.SetChainEvents([]core.ChainEvent{event1, event2, event3})
	blockChain.SetReceiptsForHash(testBlock1.Hash(), testReceipts1)
	blockChain.SetReceiptsForHash(testBlock2.Hash(), testReceipts2)

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

	service.Loop(eventsChannel)
	wg.Wait()
	if len(payloads) != 2 {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual number of payloads does not equal expected.\nactual: %+v\nexpected: 3", len(payloads))
	}

	testReceipts1Rlp, err := rlp.EncodeToBytes(testReceipts1)
	if err != nil {
		t.Error(err)
	}
	testReceipts2Rlp, err := rlp.EncodeToBytes(testReceipts2)
	if err != nil {
		t.Error(err)
	}
	expectedReceiptsRlp := [][]byte{testReceipts1Rlp, testReceipts2Rlp, nil}
	for i, payload := range payloads {
		if !bytes.Equal(payload.ReceiptsRlp, expectedReceiptsRlp[i]) {
			t.Error("Test failure:", t.Name())
			t.Logf("Actual receipt rlp for payload %d does not equal expected.\nactual: %+v\nexpected: %+v", i, payload.ReceiptsRlp, expectedReceiptsRlp[i])
		}
	}

	if !reflect.DeepEqual(builder.BlockHash, testBlock2.Hash()) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual blockhash does not equal expected.\nactual:%+v\nexpected: %+v", builder.BlockHash, testBlock2.Hash())
	}
	if !bytes.Equal(builder.OldStateRoot.Bytes(), parentBlock2.Root().Bytes()) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual root does not equal expected.\nactual:%+v\nexpected: %+v", builder.OldStateRoot, parentBlock2.Root())
	}
	if !bytes.Equal(builder.NewStateRoot.Bytes(), testBlock2.Root().Bytes()) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual root does not equal expected.\nactual:%+v\nexpected: %+v", builder.NewStateRoot, testBlock2.Root())
	}
	//look up the parent block from its hash
	expectedHashes := []common.Hash{testBlock1.ParentHash(), testBlock2.ParentHash()}
	if !reflect.DeepEqual(blockChain.ParentHashesLookedUp, expectedHashes) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual parent hash does not equal expected.\nactual:%+v\nexpected: %+v", blockChain.ParentHashesLookedUp, expectedHashes)
	}
}

func testWhenAStateDiffIsEmpty(t *testing.T) {
	//the fourth chain event causes and error (in blockchain mock)
	builder := mocks.Builder{}
	blockChain := mocks.BlockChain{}
	service := statediff.Service{
		Mutex:         sync.Mutex{},
		Builder:       &builder,
		BlockChain:    &blockChain,
		QuitChan:      make(chan bool),
		Subscriptions: make(map[rpc.ID]statediff.Subscription),
		StreamBlock:   true,
	}
	payloadChan := make(chan statediff.Payload, 2)
	quitChan := make(chan bool)
	service.Subscribe(rpc.NewID(), payloadChan, quitChan)
	blockMapping := make(map[common.Hash]*types.Block)
	blockMapping[parentBlock1.Hash()] = parentBlock1
	blockMapping[parentBlock2.Hash()] = parentBlock2
	blockChain.SetParentBlocksToReturn(blockMapping)
	blockChain.SetChainEvents([]core.ChainEvent{event1, event2, event3, event4})

	emptyStateDiff := statediff.StateDiff{
		BlockNumber: testBlock1.Number(),
		BlockHash:   testBlock1.Hash(),
	}
	accountDiff1 := statediff.AccountDiff{
		Leaf:    true,
		Key:     []byte{1, 2, 3, 4, 5, 6},
		Value:   []byte{7, 8, 9, 10, 11, 12},
		Storage: nil,
	}
	testStateDiff1 := statediff.StateDiff{
		BlockNumber:     testBlock2.Number(),
		BlockHash:       testBlock2.Hash(),
		CreatedAccounts: []statediff.AccountDiff{accountDiff1},
	}

	// has an empty []byte as the storage value
	accountDiff2 := statediff.AccountDiff{
		Leaf:    true,
		Key:     []byte{1, 2, 3, 4, 5, 6},
		Value:   []byte{},
		Storage: nil,
	}
	testStateDiff2 := statediff.StateDiff{
		BlockNumber:     testBlock3.Number(),
		BlockHash:       testBlock3.Hash(),
		CreatedAccounts: []statediff.AccountDiff{accountDiff2},
	}
	testStateDiffs := make(map[int64]statediff.StateDiff)
	testStateDiffs[testBlock1.Number().Int64()] = emptyStateDiff
	testStateDiffs[testBlock2.Number().Int64()] = testStateDiff1
	testStateDiffs[testBlock3.Number().Int64()] = testStateDiff2
	builder.SetStateDiffsToBuild(testStateDiffs)

	payloads := make([]statediff.Payload, 0, 2)
	wg := sync.WaitGroup{}
	go func() {
		wg.Add(1)
		for i := 0; i < 2; i++ {
			select {
			case payload := <-payloadChan:
				fmt.Println("got a payload")
				payloads = append(payloads, payload)
			case <-quitChan:
				fmt.Println("got a quit")
			}
		}
		wg.Done()
	}()

	service.Loop(eventsChannel)
	wg.Wait()
	if len(payloads) != 2 {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual number of payloads does not equal expected.\nactual: %+v\nexpected: 2", len(payloads))
	}

	decodedStateDiff1 := new(statediff.StateDiff)
	decode1Err := rlp.DecodeBytes(payloads[0].StateDiffRlp, decodedStateDiff1)
	if decode1Err != nil {
		t.Error("Test failure:", t.Name())
		t.Log("Error decoding StateDiffRlp from test Payload.")
	}

	if decodedStateDiff1.BlockNumber.Int64() != testBlock2.Number().Int64() {
		t.Error("Test failure:", t.Name())
		t.Logf("Test payload block number does not equal expected.\nactual: %+v\nexpected: %+v", decodedStateDiff1.BlockNumber, testBlock2.Number())
	}

	if reflect.DeepEqual(decodedStateDiff1.CreatedAccounts[0], accountDiff1) {
		t.Error("Test failure:", t.Name())
		t.Logf("Test payload block number does not equal expected.\nactual: %+v\nexpected: %+v", decodedStateDiff1.BlockNumber, testBlock2.Number())
	}

	decodedStateDiff2 := new(statediff.StateDiff)
	decode2Err := rlp.DecodeBytes(payloads[1].StateDiffRlp, decodedStateDiff2)
	if decode2Err != nil {
		t.Error("Test failure:", t.Name())
		t.Log("Error decoding StateDiffRlp from test Payload.")
	}

	if decodedStateDiff2.BlockNumber.Int64() != testBlock3.Number().Int64() {
		t.Error("Test failure:", t.Name())
		t.Logf("Test payload block number does not equal expected.\nactual: %+v\nexpected: %+v", decodedStateDiff2.BlockNumber, testBlock3.Number())
	}

	if reflect.DeepEqual(decodedStateDiff2.CreatedAccounts[0], accountDiff2) {
		t.Error("Test failure:", t.Name())
		t.Logf("Test payload block number does not equal expected.\nactual: %+v\nexpected: %+v", decodedStateDiff2.BlockNumber, testBlock3.Number())
	}
}

func testErrorInBlockLoop(t *testing.T) {
	//second block's parent block can't be found
	builder := mocks.Builder{}
	blockChain := mocks.BlockChain{}
	service := statediff.Service{
		Builder:       &builder,
		BlockChain:    &blockChain,
		QuitChan:      make(chan bool),
		Subscriptions: make(map[rpc.ID]statediff.Subscription),
	}
	payloadChan := make(chan statediff.Payload)
	quitChan := make(chan bool)
	service.Subscribe(rpc.NewID(), payloadChan, quitChan)
	blockMapping := make(map[common.Hash]*types.Block)
	blockMapping[parentBlock1.Hash()] = parentBlock1
	blockChain.SetParentBlocksToReturn(blockMapping)
	blockChain.SetChainEvents([]core.ChainEvent{event1, event2})
	// Need to have listeners on the channels or the subscription will be closed and the processing halted
	go func() {
		select {
		case <-payloadChan:
		case <-quitChan:
		}
	}()
	service.Loop(eventsChannel)

	if !bytes.Equal(builder.BlockHash.Bytes(), testBlock1.Hash().Bytes()) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual does not equal expected.\nactual:%+v\nexpected: %+v", builder.BlockHash, testBlock1.Hash())
	}
	if !bytes.Equal(builder.OldStateRoot.Bytes(), parentBlock1.Root().Bytes()) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual does not equal expected.\nactual:%+v\nexpected: %+v", builder.OldStateRoot, parentBlock1.Root())
	}
	if !bytes.Equal(builder.NewStateRoot.Bytes(), testBlock1.Root().Bytes()) {
		t.Error("Test failure:", t.Name())
		t.Logf("Actual does not equal expected.\nactual:%+v\nexpected: %+v", builder.NewStateRoot, testBlock1.Root())
	}
}
