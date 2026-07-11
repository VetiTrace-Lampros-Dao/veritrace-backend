package onchain

import (
	"context"
	"encoding/hex"
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
	},
	{
		"inputs": [
			{"name": "sha256hash", "type": "bytes32"}
		],
		"name": "verifyContent",
		"outputs": [
			{"name": "creator", "type": "address"},
			{"name": "timestamp", "type": "uint64"},
			{"name": "phash", "type": "uint64"},
			{"name": "ipfsCid", "type": "string"},
			{"name": "aitool", "type": "string"}
		],
		"stateMutability": "view",
		"type": "function"
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

func (v *Verifier) VerifyHash(ctx context.Context, sha256Hex string) (*OnChainRecord, error) {
	cleaned := sha256Hex
	if strings.HasPrefix(cleaned, "0x") {
		cleaned = cleaned[2:]
	}
	hashRaw, err := hex.DecodeString(cleaned)
	if err != nil {
		return nil, fmt.Errorf("invalid sha256 hex: %w", err)
	}

	var hashBytes32 [32]byte
	copy(hashBytes32[:], hashRaw)

	callData, err := v.parsedABI.Pack("verifyContent", hashBytes32)
	if err != nil {
		return nil, fmt.Errorf("failed to pack call data: %w", err)
	}

	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	result, err := v.client.CallContract(callCtx, ethereum.CallMsg{
		To:   &v.contractAddr,
		Data: callData,
	}, nil)
	if err != nil {
		return nil, nil
	}

	if len(result) == 0 {
		return nil, nil
	}

	outputs, err := v.parsedABI.Unpack("verifyContent", result)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack verifyContent response: %w", err)
	}
	if len(outputs) < 4 {
		return nil, fmt.Errorf("unexpected output count from contract: %d", len(outputs))
	}

	creator, ok := outputs[0].(common.Address)
	if !ok {
		return nil, fmt.Errorf("unexpected type for creator output")
	}
	ipfsCid, ok := outputs[3].(string)
	if !ok {
		return nil, fmt.Errorf("unexpected type for ipfsCid output")
	}

	return &OnChainRecord{
		Sha256Hash: sha256Hex,
		Creator:    creator.Hex(),
		IpfsCid:    ipfsCid,
		TxHash:     "",
	}, nil
}
