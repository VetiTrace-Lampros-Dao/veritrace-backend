package onchain

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/VetiTrace-Lampros-Dao/veritrace-backend/config"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
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

type OnChainRecord struct {
	Sha256Hash string
	Creator    string
	IpfsCid    string
	TxHash     string
}

type Verifier struct {
	client       *ethclient.Client
	contractAddr common.Address
	parsedABI    abi.ABI
}

func NewVerifier(cfg *config.Config) (*Verifier, error) {
	client, err := ethclient.Dial(cfg.ArbitrumWS)
	if err != nil {
		return nil, fmt.Errorf("failed to dial WebSocket: %w", err)
	}

	parsedABI, err := abi.JSON(strings.NewReader(contractABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse contract ABI: %w", err)
	}

	return &Verifier{
		client:       client,
		contractAddr: common.HexToAddress(cfg.ContractAddress),
		parsedABI:    parsedABI,
	}, nil
}

func (v *Verifier) Close() {
	if v.client != nil {
		v.client.Close()
	}
}

// VerifyHash queries the blockchain for the ContentRegistered event of a specific sha256hash.
func (v *Verifier) VerifyHash(ctx context.Context, sha256Hex string) (*OnChainRecord, error) {
	if strings.HasPrefix(sha256Hex, "0x") {
		sha256Hex = sha256Hex[2:]
	}
	hashBytes := common.HexToHash(sha256Hex)

	query := ethereum.FilterQuery{
		Addresses: []common.Address{v.contractAddr},
		Topics: [][]common.Hash{
			{v.parsedABI.Events["ContentRegistered"].ID},
			{hashBytes},
		},
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	
	logs, err := v.client.FilterLogs(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch logs: %w", err)
	}

	if len(logs) == 0 {
		return nil, nil // Not found on chain
	}

	lastLog := logs[len(logs)-1]

	var event struct {
		Phash     uint64
		Timestamp uint64
		IpfsCid   string
		Aitool    string
	}

	err = v.parsedABI.UnpackIntoInterface(&event, "ContentRegistered", lastLog.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack event data: %w", err)
	}

	creator := common.BytesToAddress(lastLog.Topics[2].Bytes()).Hex()

	return &OnChainRecord{
		Sha256Hash: sha256Hex,
		Creator:    creator,
		IpfsCid:    event.IpfsCid,
		TxHash:     lastLog.TxHash.Hex(),
	}, nil
}
