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

package mocks

import (
	"errors"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/event"
)

// BlockChain is a mock blockchain for testing
type BlockChain struct {
	ParentHashesLookedUp []common.Hash
	parentBlocksToReturn map[common.Hash]*types.Block
	callCount            int
	ChainEvents          []core.ChainEvent
	Receipts             map[common.Hash]types.Receipts
}

// AddToStateDiffProcessedCollection mock method
func (blockChain *BlockChain) AddToStateDiffProcessedCollection(hash common.Hash) {}

// SetParentBlocksToReturn mock method
func (blockChain *BlockChain) SetParentBlocksToReturn(blocks map[common.Hash]*types.Block) {
	if blockChain.parentBlocksToReturn == nil {
		blockChain.parentBlocksToReturn = make(map[common.Hash]*types.Block)
	}
	blockChain.parentBlocksToReturn = blocks
}

// GetBlockByHash mock method
func (blockChain *BlockChain) GetBlockByHash(hash common.Hash) *types.Block {
	blockChain.ParentHashesLookedUp = append(blockChain.ParentHashesLookedUp, hash)

	var parentBlock *types.Block
	if len(blockChain.parentBlocksToReturn) > 0 {
		parentBlock = blockChain.parentBlocksToReturn[hash]
	}

	return parentBlock
}

// SetChainEvents mock method
func (blockChain *BlockChain) SetChainEvents(chainEvents []core.ChainEvent) {
	blockChain.ChainEvents = chainEvents
}

// SubscribeChainEvent mock method
func (blockChain *BlockChain) SubscribeChainEvent(ch chan<- core.ChainEvent) event.Subscription {
	subErr := errors.New("Subscription Error")

	subscription := event.NewSubscription(func(quit <-chan struct{}) error {
		for index, chainEvent := range blockChain.ChainEvents {
			fmt.Println("event index", index)
			if index == len(blockChain.ChainEvents)- 1{
				time.Sleep(250 * time.Millisecond)
				return subErr
			}
			select {
			case ch <- chainEvent:
			case <-quit:
				fmt.Println("are we getting here?")
				return nil
			}
		}
		return nil
	})

	return subscription
}

// SetReceiptsForHash mock method
func (blockChain *BlockChain) SetReceiptsForHash(hash common.Hash, receipts types.Receipts) {
	if blockChain.Receipts == nil {
		blockChain.Receipts = make(map[common.Hash]types.Receipts)
	}
	blockChain.Receipts[hash] = receipts
}

// GetReceiptsByHash mock method
func (blockChain *BlockChain) GetReceiptsByHash(hash common.Hash) types.Receipts {
	return blockChain.Receipts[hash]
}
