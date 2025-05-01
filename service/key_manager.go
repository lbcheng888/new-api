package service

import (
	"context"
	"errors"
	"one-api/common"
	"strings"
	"sync"
	"time"
)

var (
	ErrNoAvailableKey = errors.New("没有符合速率限制的可用 API 密钥")
	DailyLimitPerKey  = int(25) // 每个 key 每天的请求限制
)

// KeyManager manages API keys and their usage rate limits.
type KeyManager struct {
	keys         []string
	usage        map[string][]int64 // key: API key, value: list of request timestamps (for time-window limit)
	mu           sync.Mutex
	rateLimit    int           // Max requests per time window
	timeWindow   time.Duration // Duration of the time window
	nextKeyIndex int           // Index to start checking for the next available key (for round-robin)
}

// NewKeyManager creates a new KeyManager.
func NewKeyManager(keysStr string, rateLimit int, timeWindow time.Duration) *KeyManager {
	keys := make([]string, 0)
	if strings.Contains(keysStr, "\n") {
		for _, k := range strings.Split(keysStr, "\n") {
			trimmedKey := strings.TrimSpace(k)
			if trimmedKey != "" {
				keys = append(keys, trimmedKey)
			}
		}
	} else if strings.Contains(keysStr, " ") {
		for _, k := range strings.Split(keysStr, " ") {
			trimmedKey := strings.TrimSpace(k)
			if trimmedKey != "" {
				keys = append(keys, trimmedKey)
			}
		}
	} else {
		keys = append(keys, keysStr)
	}
	return &KeyManager{
		keys:         keys,
		usage:        make(map[string][]int64),
		rateLimit:    rateLimit,
		timeWindow:   timeWindow,
		nextKeyIndex: 0, // Initialize nextKeyIndex
	}
}

func (km *KeyManager) GetAvailableKey(ctx context.Context) (string, error) {
	km.mu.Lock() // Lock for accessing shared KeyManager state (usage map, nextKeyIndex)
	defer km.mu.Unlock()

	now := time.Now()
	nowUnix := now.Unix()
	// cutoff := nowUnix - int64(km.timeWindow.Seconds())
	// todayStr := now.Format("20060102")

	numKeys := len(km.keys)
	if numKeys == 0 {
		return "", ErrNoAvailableKey // No keys configured
	}

	// Start checking from nextKeyIndex and loop through all keys once
	for i := 0; i < numKeys; i++ {
		currentIndex := (km.nextKeyIndex + i) % numKeys
		key := km.keys[currentIndex]

		// timestamps := km.usage[key]
		// validTimestamps := make([]int64, 0, len(timestamps))
		// for _, ts := range timestamps {
		// 	if ts >= cutoff {
		// 		validTimestamps = append(validTimestamps, ts)
		// 	}
		// }
		// km.usage[key] = validTimestamps
		// if len(validTimestamps) >= km.rateLimit {
		// 	continue
		// }
		// dailyCount := 0
		// redisKey := fmt.Sprintf("daily_limit:%s:%d", todayStr, currentIndex)
		// dailyCount, err := common.RDB.Get(ctx, redisKey).Int()
		// if err != nil {
		// 	common.RDB.Set(ctx, redisKey, 0, 0)
		// 	dailyCount = 0
		// }

		// if dailyCount >= DailyLimitPerKey {
		// 	common.LogWarn(ctx, fmt.Sprintf("%d %s daily limit reached (%d/%d)", currentIndex, key, dailyCount, DailyLimitPerKey))
		// 	continue
		// }

		// newDailyCount, incrErr := common.RDB.Incr(ctx, redisKey).Result() // Use common.RDB
		// if incrErr != nil {
		// 	common.LogError(ctx, fmt.Sprintf("Redis INCR error for key %s: %v", redisKey, incrErr))
		// 	continue
		// }

		// if newDailyCount == 1 {
		// 	expireErr := common.RDB.Expire(ctx, redisKey, 24*time.Hour+5*time.Minute).Err() // Use common.RDB
		// 	if expireErr != nil {
		// 		common.LogError(ctx, fmt.Sprintf("Redis EXPIRE error for key %s: %v", redisKey, expireErr))
		// 	}
		// }

		km.usage[key] = append(km.usage[key], nowUnix)

		km.nextKeyIndex = (currentIndex + 1) % numKeys
		return key, nil
	}

	// Looped through all keys, none available
	common.LogWarn(ctx, "No available key found after checking all keys.")
	return "", ErrNoAvailableKey
}

// RecordUsage is deprecated as usage recording is now handled within GetAvailableKey.
// func (km *KeyManager) RecordUsage(key string) {
// 	km.mu.Lock()
// 	defer km.mu.Unlock()

// 	now := time.Now().Unix()
// 	cutoff := now - int64(km.timeWindow.Seconds())

// 	// Append new timestamp
// 	timestamps := km.usage[key]
// 	timestamps = append(timestamps, now)

// 	// Clean up old timestamps
// 	validTimestamps := make([]int64, 0, len(timestamps))
// 	for _, ts := range timestamps {
// 		if ts >= cutoff {
// 			validTimestamps = append(validTimestamps, ts)
// 		}
// 	}
// 	km.usage[key] = validTimestamps
// }

// GetKeys returns the list of keys managed by this manager.
func (km *KeyManager) GetKeys() []string {
	// Return a copy to prevent external modification
	keysCopy := make([]string, len(km.keys))
	copy(keysCopy, km.keys)
	return keysCopy
}
