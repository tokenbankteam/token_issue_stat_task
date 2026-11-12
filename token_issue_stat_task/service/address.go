package service

import (
	"strings"

	"github.com/tokenbankteam/tp-base-server/pkg/consts"
)

// checkAddressAndToLower 与 tp-base-server/pkg/utils.CheckAddressAndToLower 行为一致：
// namespace 是 ETHEREUM，或链是 eth / heco / bsc / okc(新) / klaytn 时，地址统一转小写；
// 其它链（solana、tron、sui、aptos…）地址大小写敏感，原样返回。
//
// 这里复制一份实现而不是直接 import 那个 utils 包：utils 里还有 address_evm.go /
// address_ton.go 等文件，会把 go-ethereum、tonutils 这些重依赖拉进来，对一个只做
// MySQL → Kafka 的扫描任务来说不划算。常量仍然复用 tp-base-server/pkg/consts（纯常量，无依赖）。
func checkAddressAndToLower(address string, blockchainId int64, namespace string) string {
	if namespace == consts.NamespaceETHEREUM ||
		blockchainId == consts.BlockChainEth ||
		blockchainId == consts.BlockChainHeco ||
		blockchainId == consts.BlockChainBsc ||
		blockchainId == consts.BlockChainOkChainNew ||
		blockchainId == consts.BlockChainKlaytn {
		return strings.ToLower(address)
	}
	return address
}
