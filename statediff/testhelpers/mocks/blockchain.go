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

	"time"

	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/event"
)

// BlockChain is a mock blockchain for testing
type BlockChain struct {
	callCount         int
	StateChangeEvents []core.StateChangeEvent
}

func (blockChain *BlockChain) SubscribeStateChangeEvents(ch chan<- core.StateChangeEvent) event.Subscription {
	subErr := errors.New("Subscription Error")

	var eventCounter int
	subscription := event.NewSubscription(func(quit <-chan struct{}) error {
		for _, stateChangeEvent := range blockChain.StateChangeEvents {
			if eventCounter > 1 {
				time.Sleep(250 * time.Millisecond)
				return subErr
			}
			select {
			case ch <- stateChangeEvent:
			case <-quit:
				return nil
			}
			eventCounter++
		}
		return nil
	})

	return subscription
}

// Mock method for setting StateChangeEvents to return
func (blockChain *BlockChain) SetStateChangeEvents(stateChangeEvents []core.StateChangeEvent) {
	blockChain.StateChangeEvents = stateChangeEvents
}
