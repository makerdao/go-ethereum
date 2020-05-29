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

package statediff

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
)

const stateChangeEventChanSize = 20000

type blockChain interface {
	SubscribeStateChangeEvents(ch chan<- core.StateChangeEvent) event.Subscription
}

// IService is the state-diffing service interface
type IService interface {
	// APIs(), Protocols(), Start() and Stop()
	node.Service
	// Main event loop for processing state diffs
	Loop(stateChangeEventCh chan core.StateChangeEvent)
	// Method to subscribe to receive state diff processing output
	Subscribe(id rpc.ID, sub chan<- Payload, quitChan chan<- bool)
	// Method to unsubscribe from state diff processing
	Unsubscribe(id rpc.ID) error
}

// Service is the underlying struct for the state diffing service
type Service struct {
	// Used to sync access to the Subscriptions
	sync.Mutex
	// Used to build the state diff objects
	// Used to subscribe to chain events (blocks)
	BlockChain blockChain
	// Used to signal shutdown of the service
	QuitChan chan bool
	// A mapping of rpc.IDs to their subscription channels
	Subscriptions map[rpc.ID]Subscription
	// Addresses for contracts we care about - only sending diffs for these to the subscription
	WatchedAddresses []string
	// Whether or not we have any subscribers; only if we do, do we processes state diffs
	subscribers int32
}

// NewStateDiffService creates a new statediff.Service
func NewStateDiffService(db ethdb.Database, blockChain *core.BlockChain, config Config) (*Service, error) {
	return &Service{
		Mutex:            sync.Mutex{},
		BlockChain:       blockChain,
		QuitChan:         make(chan bool),
		Subscriptions:    make(map[rpc.ID]Subscription),
		WatchedAddresses: config.WatchedAddresses,
	}, nil
}

// Protocols exports the services p2p protocols, this service has none
func (sds *Service) Protocols() []p2p.Protocol {
	return []p2p.Protocol{}
}

// APIs returns the RPC descriptors the statediff.Service offers
func (sds *Service) APIs() []rpc.API {
	return []rpc.API{
		{
			Namespace: APIName,
			Version:   APIVersion,
			Service:   NewPublicStateDiffAPI(sds),
			Public:    true,
		},
	}
}

// Loop is the main processing method
func (sds *Service) Loop(stateChangeEventCh chan core.StateChangeEvent) {
	stateChangeEventsSub := sds.BlockChain.SubscribeStateChangeEvents(stateChangeEventCh)
	defer stateChangeEventsSub.Unsubscribe()

	errCh := stateChangeEventsSub.Err()
	for {
		select {
		//Notify stateChangeEvent channel of events
		case stateChangeEvent := <-stateChangeEventCh:
			log.Debug("Event received from stateChangeEventCh", "block number", stateChangeEvent.Block.Number(), "event", stateChangeEvent)
			stateChanges := sds.filterByWatchedAddresses(stateChangeEvent.StateChanges)

			payload, processingErr := processStateChanges(stateChanges, stateChangeEvent.Block)
			if processingErr != nil {
				// The service loop continues even if processing one StateChangeEvent fails
				log.Error(fmt.Sprintf("Error processing state for block %d; error: %s ",
					stateChangeEvent.Block.Number(), processingErr.Error()))
			}

			isEmpty := isEmptyPayload(payload)
			// Send a payload to subscribers only if isn't empty
			if !isEmpty {
				sds.send(payload)
			}
		case err := <-errCh:
			log.Warn("Error from state change event subscription, breaking loop", "error", err)
			sds.close()
			return
		case <-sds.QuitChan:
			log.Info("Quitting the statediffing process")
			sds.close()
			return
		}
	}
}

// processStateChanges builds the state diff Payload from the modified accounts in the StateChangeEvent
func processStateChanges(stateChanges state.StateChanges, block *types.Block) (Payload, error) {
	var accountDiffs []AccountDiff
	var emptyPayload Payload

	// Iterate over state changes to build AccountDiffs
	for addr, modifiedAccount := range stateChanges {
		a, err := buildAccountDiff(addr, modifiedAccount)
		if err != nil {
			return emptyPayload, err
		}

		accountDiffs = append(accountDiffs, a)
	}

	if len(accountDiffs) == 0 {
		return emptyPayload, nil
	}

	stateDiff := StateDiff{
		BlockNumber:     block.Number(),
		BlockHash:       block.Hash(),
		UpdatedAccounts: accountDiffs,
	}

	stateDiffRlp, err := rlp.EncodeToBytes(stateDiff)
	if err != nil {
		return emptyPayload, err
	}
	payload := Payload{
		StateDiffRlp: stateDiffRlp,
	}

	return payload, nil
}

// buildAccountDiff
func buildAccountDiff(addr common.Address, modifiedAccount state.ModifiedAccount) (AccountDiff, error) {
	emptyAccountDiff := AccountDiff{}
	accountBytes, err := rlp.EncodeToBytes(modifiedAccount.Account)
	if err != nil {
		return emptyAccountDiff, err
	}

	var storageDiffs []StorageDiff
	for k, v := range modifiedAccount.Storage {
		// Storage diff value should be an RLP object too
		encodedValueRlp, err := rlp.EncodeToBytes(v[:])
		if err != nil {
			return emptyAccountDiff, err
		}
		storageKey := k
		diff := StorageDiff{
			Key:   storageKey[:],
			Value: encodedValueRlp,
		}
		storageDiffs = append(storageDiffs, diff)
	}

	address := addr
	return AccountDiff{
		Key:     address[:],
		Value:   accountBytes,
		Storage: storageDiffs,
	}, nil
}

func (sds *Service) addressInWatchedAddresses(address common.Address) bool {
	for _, watchedAddress := range sds.WatchedAddresses {
		checkSummedWatchedAddress := common.HexToAddress(watchedAddress).Hex()
		if checkSummedWatchedAddress == address.Hex() {
			return true
		}
	}

	return false
}

func (sds *Service) filterByWatchedAddresses(stateChanges state.StateChanges) state.StateChanges {
	if len(sds.WatchedAddresses) > 0 {
		filteredStateChanges := make(state.StateChanges)
		for addr, modifiedAccount := range stateChanges {
			if sds.addressInWatchedAddresses(addr) {
				filteredStateChanges[addr] = modifiedAccount
			}
		}
		return filteredStateChanges
	} else {
		return stateChanges
	}
}

func isEmptyPayload(payload Payload) bool {
	emptyPayload := Payload{}
	return reflect.DeepEqual(payload, emptyPayload)
}

// Subscribe is used by the API to subscribe to the service loop
func (sds *Service) Subscribe(id rpc.ID, sub chan<- Payload, quitChan chan<- bool) {
	log.Info("Subscribing to the statediff service")
	if atomic.CompareAndSwapInt32(&sds.subscribers, 0, 1) {
		log.Info("State diffing subscription received; beginning statediff processing")
	}
	sds.Lock()
	sds.Subscriptions[id] = Subscription{
		PayloadChan: sub,
		QuitChan:    quitChan,
	}
	sds.Unlock()
}

// Unsubscribe is used to unsubscribe from the service loop
func (sds *Service) Unsubscribe(id rpc.ID) error {
	log.Info("Unsubscribing from the statediff service")
	sds.Lock()
	_, ok := sds.Subscriptions[id]
	if !ok {
		return fmt.Errorf("cannot unsubscribe; subscription for id %s does not exist", id)
	}
	delete(sds.Subscriptions, id)
	if len(sds.Subscriptions) == 0 {
		if atomic.CompareAndSwapInt32(&sds.subscribers, 1, 0) {
			log.Info("No more subscriptions; halting statediff processing")
		}
	}
	sds.Unlock()
	return nil
}

// Start is used to begin the service
func (sds *Service) Start(*p2p.Server) error {
	log.Info("Starting statediff service")

	stateChangeEventCh := make(chan core.StateChangeEvent, stateChangeEventChanSize)
	go sds.Loop(stateChangeEventCh)

	return nil
}

// Stop is used to close down the service
func (sds *Service) Stop() error {
	log.Info("Stopping statediff service")
	close(sds.QuitChan)
	return nil
}

// send is used to fan out and serve the payloads to all subscriptions
func (sds *Service) send(payload Payload) {
	sds.Lock()
	for id, sub := range sds.Subscriptions {
		select {
		case sub.PayloadChan <- payload:
			log.Debug(fmt.Sprintf("sending state diff payload to subscription %s", id))
		default:
			log.Info(fmt.Sprintf("unable to send payload to subscription %s; channel has no receiver", id))
			// in this case, try to close the bad subscription and remove it
			select {
			case sub.QuitChan <- true:
				log.Info(fmt.Sprintf("closing subscription %s", id))
			default:
				log.Info(fmt.Sprintf("unable to close subscription %s; channel has no receiver", id))
			}
			delete(sds.Subscriptions, id)
		}
	}
	// If after removing all bad subscriptions we have none left, halt processing
	if len(sds.Subscriptions) == 0 {
		if atomic.CompareAndSwapInt32(&sds.subscribers, 1, 0) {
			log.Info("No more subscriptions; halting statediff processing")
		}
	}
	sds.Unlock()
}

// close is used to close all listening subscriptions
func (sds *Service) close() {
	sds.Lock()
	for id, sub := range sds.Subscriptions {
		select {
		case sub.QuitChan <- true:
			log.Info(fmt.Sprintf("closing subscription %s", id))
		default:
			log.Info(fmt.Sprintf("unable to close subscription %s; channel has no receiver", id))
		}
		delete(sds.Subscriptions, id)
	}
	sds.Unlock()
}
