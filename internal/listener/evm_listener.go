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

	go func() {
		defer close(l.eventLog)

		for {
			select {
			case <-ctx.Done():
				return
			default:
				if l.client == nil {
					client, err := ethclient.Dial(l.cfg.ArbitrumWS)
					if err != nil {
						log.Printf("EVM Listener: Dial failed: %v. Retrying in 5s...", err)
						time.Sleep(5 * time.Second)
						continue
					}
					l.client = client
				}

				logsChan := make(chan types.Log)
				sub, err := l.client.SubscribeFilterLogs(ctx, query, logsChan)
				if err != nil {
					log.Printf("EVM Listener: Subscription failed: %v. Retrying in 5s...", err)
					l.client.Close()
					l.client = nil
					time.Sleep(5 * time.Second)
					continue
				}

				log.Printf("EVM Listener: Successfully connected & subscribed to Arbitrum Sepolia")

				errChan := sub.Err()
				keepRunning := true
				for keepRunning {
					select {
					case <-ctx.Done():
						sub.Unsubscribe()
						return
					case subErr := <-errChan:
						if subErr != nil {
							log.Printf("EVM subscription error: %v. Reconnecting...", subErr)
						}
						sub.Unsubscribe()
						l.client.Close()
						l.client = nil
						keepRunning = false
						time.Sleep(2 * time.Second)
					case vLog := <-logsChan:
						log.Printf("EVM Listener: Received raw event! Tx: %s, Block: %d, Address: %s", vLog.TxHash.Hex(), vLog.BlockNumber, vLog.Address.Hex())
						var event struct {
							Phash     uint64
							Timestamp uint64
							IpfsCid   string
							Aitool    string
						}

						err := parsedABI.UnpackIntoInterface(&event, "ContentRegistered", vLog.Data)
						if err != nil {
							log.Printf("EVM Listener: Failed to unpack event data: %v", err)
							continue
						}

						if len(vLog.Topics) < 3 {
							log.Printf("EVM Listener: Insufficient topics count (%d)", len(vLog.Topics))
							continue
						}

						sha256hash := vLog.Topics[1].Hex()
						creator := common.BytesToAddress(vLog.Topics[2].Bytes()).Hex()

						log.Printf("EVM Listener: Unpacked successfully! Sha256Hash: %s, Creator: %s, PHash: %d, IpfsCid: %s, AiTool: %s", sha256hash, creator, event.Phash, event.IpfsCid, event.Aitool)

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
			}
		}
	}()

	return nil
}
