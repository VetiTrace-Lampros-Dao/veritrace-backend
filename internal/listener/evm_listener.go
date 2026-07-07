package listener

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/config"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

const contractABI = `[
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true, "name": "sha256hash", "type": "bytes32"},
			{"indexed": true, "name": "creator", "type": "address"},
			{"indexed": false, "name": "phash", "type": "uint64"},
			{"indexed": false, "name": "timestamp", "type": "uint64"},
			{"indexed": false, "name": "ipfsCid", "type": "string"},
			{"indexed": false, "name": "aitool", "type": "string"}
		],
		"name": "ContentRegistered",
		"type": "event"
	}
]`

type EventPayload struct {
	Sha256Hash     string
	CreatorAddress string
	PHash          uint64
	Timestamp      uint64
	IpfsCid        string
	AiTool         string
}

type EVMListener struct {
	cfg      *config.Config
	client   *ethclient.Client
	eventLog chan EventPayload
}

func NewEVMListener(cfg *config.Config) (*EVMListener, error) {
	client, err := ethclient.Dial(cfg.ArbitrumWS)
	if err != nil {
		return nil, fmt.Errorf("failed to dial WebSocket: %w", err)
	}

	return &EVMListener{
		cfg:      cfg,
		client:   client,
		eventLog: make(chan EventPayload, 100),
	}, nil
}

func (l *EVMListener) Events() <-chan EventPayload {
	return l.eventLog
}

func (l *EVMListener) Close() {
	if l.client != nil {
		l.client.Close()
	}
}

func (l *EVMListener) Start(ctx context.Context) error {
	parsedABI, err := abi.JSON(strings.NewReader(contractABI))
	if err != nil {
		return fmt.Errorf("failed to parse contract ABI: %w", err)
	}

	contractAddr := common.HexToAddress(l.cfg.ContractAddress)
	query := ethereum.FilterQuery{
		Addresses: []common.Address{contractAddr},
		Topics: [][]common.Hash{
			{parsedABI.Events["ContentRegistered"].ID},
		},
	}

	logsChan := make(chan types.Log)
	sub, err := l.client.SubscribeFilterLogs(ctx, query, logsChan)
	if err != nil {
		return fmt.Errorf("failed to subscribe to contract logs: %w", err)
	}

	go func() {
		defer sub.Unsubscribe()
		defer close(l.eventLog)

		for {
			select {
			case <-ctx.Done():
				return
			case err := <-sub.Err():
				if err != nil {
					log.Printf("EVM subscription error: %v", err)
				}
				time.Sleep(2 * time.Second)
				return
			case vLog := <-logsChan:
				var event struct {
					Phash     uint64
					Timestamp uint64
					IpfsCid   string
					Aitool    string
				}

				err := parsedABI.UnpackIntoInterface(&event, "ContentRegistered", vLog.Data)
				if err != nil {
					log.Printf("Failed to unpack event: %v", err)
					continue
				}

				if len(vLog.Topics) < 3 {
					log.Println("Insufficient topics in log")
					continue
				}

				sha256hash := vLog.Topics[1].Hex()
				creator := common.BytesToAddress(vLog.Topics[2].Bytes()).Hex()

				l.eventLog <- EventPayload{
					Sha256Hash:     sha256hash,
					CreatorAddress: creator,
					PHash:          event.Phash,
					Timestamp:      event.Timestamp,
					IpfsCid:        event.IpfsCid,
					AiTool:         event.Aitool,
				}
			}
		}
	}()

	return nil
}
