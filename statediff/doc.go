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

/*
Package statediff provides an auxiliary service that processes state diff objects from incoming chain events,
relaying the objects to any rpc subscriptions.

The service is spun up using the below CLI flags
--statediff: boolean flag, turns on the service
--statediff.watchedaddresses: string slice flag, used to limit the state diffing process to the given addresses. Usage: --statediff.watchedaddresses=addr1 --statediff.watchedaddresses=addr2 --statediff.watchedaddresses=addr3

If you wish to use the websocket endpoint to subscribe to the statediff service, be sure to open up the Websocket RPC server with the `--ws` flag. The IPC-RPC server is turned on by default.

The statediffing services works only with `--syncmode="full", but -importantly- does not require garbage collection to be turned off (does not require an archival node).

e.g.

$ ./geth --statediff --ws --syncmode "full"

This starts up the geth node in full sync mode, starts up the statediffing service, and opens up the websocket endpoint to subscribe to the service.

Rpc subscriptions to the service can be created using the rpc.Client.Subscribe() method,
with the "statediff" namespace, a statediff.Payload channel, and the name of the statediff api's rpc method- "stream".

e.g.

client, _ := rpc.Dial("ipcPathOrWsURL")
stateDiffPayloadChan := make(chan statediff.Payload, 20000)
subscription, err := client.Subscribe(context.Background(), "statediff", stateDiffPayloadChan, "stream"})
for {
	select {
	case subscriptionErr := <-subscription.Err():
		log.Error(subscriptionErr)
	case stateDiffPayload := <- stateDiffPayloadChan:
		processPayload(stateDiffPayload)
	}
}
*/
package statediff
