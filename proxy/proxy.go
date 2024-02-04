package proxy

import (
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"
)

type RGRequest struct {
	rid  string
	op   string
	keys []string
	vals []string
}

type ServerReadResponse struct {
	idx string
	val string
}

var C int = 1024
var R int = 512
var fD int = 10
var B int = 10

var cache, _ = lru.New[string, string](C + R)

var ts int = 0
var ts_lock sync.Mutex

func getIndex(plaintextKey string) string { //Make ciphertext key by encrypting plaintext key and timestamp
	//TODO: IMPLEMENT
	return ""
}

func objectIsReal(key string) bool {
	//TODO: IMPLEMENT
	return true
}

func getDummyVal() string {
	//TODO: IMPLEMENT
	return ""
}

func initCache() {
	//TODO: IMPLEMENT
}

func handleJob(requests []RGRequest) {
	dedupResp := make(map[string]map[string]bool)
	cliResp := make(map[string]string)

	ts_lock.Lock()
	curr_ts := ts
	ts += 1
	ts_lock.Unlock()

	for i := 0; i < len(requests); i++ {
		curr_request := requests[i]
		rid := curr_request.rid

		for waffleKeyIdx := 0; waffleKeyIdx < len(curr_request.keys); waffleKeyIdx++ {
			curr_key := curr_request.keys[i] // The "plaintext key"
			curr_val := curr_request.vals[i]

			cached_val, cache_contains_key := cache.Get(curr_key)
			if curr_request.op == "read" && cache_contains_key {
				cliResp[rid] = cached_val
			} else {
				if _, key_in_dedup := dedupResp[curr_key]; !key_in_dedup {
					dedupResp[curr_key] = map[string]bool{}
				}
				dedupResp[curr_key][rid] = true
			}

			if curr_request.op == "write" {
				if !cache_contains_key {
					dedupResp[curr_key][rid] = false
				}

				cache.Add(curr_key, curr_val) // Should not result in eviction
				cliResp[rid] = cached_val
			}
		}
	}

	readBatch := make(map[string]string)
	for k, _ := range dedupResp {
		readBatch[getIndex(k)] = k
		//set timestamp in BST for k -- BST.setTimestamp(𝑘, 𝑡𝑠)
	}

	for fake_dummy_query_idx := 0; fake_dummy_query_idx < fD; fake_dummy_query_idx++ {
		k := "LMBASOMB" //BST.getMinTimestampObj(𝑑𝑢𝑚𝑚𝑦)
		readBatch[getIndex(k)] = k
		//set timestamp in BST for k -- BST.setTimestamp(𝑘, 𝑡𝑠)
	}

	R := len(dedupResp)
	for fake_real_query_idx := 0; fake_real_query_idx < B-(R+fD); fake_real_query_idx++ {
		k := "LMBASOMB" //BST.getMinTimestampObj(𝑑𝑢𝑚𝑚𝑦)
		readBatch[getIndex(k)] = k
		//set timestamp in BST for k -- BST.setTimestamp(𝑘, 𝑡𝑠)
	}

	//TODO: SEND READ BATCH KEYS TO SERVER
	resp := []ServerReadResponse{}

	writeBatch := make(map[string]string)
	for i := 0; i < len(resp); i++ {
		curr_resp := resp[i]
		idx := curr_resp.idx //Ciphertext key
		val := curr_resp.val

		k := readBatch[idx] //Plaintext key
		if cache.Contains(k) {
			for rid, need_resp := range dedupResp[k] {
				if need_resp {
					cliResp[rid] = val
				}
			}
		}

		if objectIsReal(k) {
			oldest_key, oldest_val, ok := cache.RemoveOldest()
			if ok {
				writeBatch[getIndex(oldest_key)] = oldest_val

				//𝑣𝑎𝑙′ ← 𝑐𝑎𝑐ℎ𝑒 [𝑘] if 𝑘 in 𝑐𝑎𝑐ℎ𝑒 else 𝑣𝑎𝑙
				//𝑐𝑎𝑐ℎ𝑒 [𝑘] ← 𝑣𝑎𝑙′
				cached_val, cache_contains_key := cache.Get(k)
				// Should not cause any eviction
				if cache_contains_key {
					cache.Add(k, cached_val)
				} else {
					cache.Add(k, val)
				}
			}
		} else {
			writeBatch[getIndex(k)] = getDummyVal()
		}
	}

	//TODO: SEND WRITE BATCH TO SERVER
}
