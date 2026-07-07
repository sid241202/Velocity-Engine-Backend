package services

import (
	"context"
	"errors"
	"testing"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"
)

// resetRBACCacheForTest clears the permission cache and restores fetchFunc
// after the test — tests must not leak state into each other or into the
// real fetchUserPermissionsFromDB.
func resetRBACCacheForTest(t *testing.T) {
	t.Helper()
	permCacheMu.Lock()
	permCache = make(map[int64]*userPermissionCacheEntry)
	permCacheMu.Unlock()
	origFetch := fetchFunc
	t.Cleanup(func() {
		permCacheMu.Lock()
		permCache = make(map[int64]*userPermissionCacheEntry)
		permCacheMu.Unlock()
		fetchFunc = origFetch
	})
}

func withTTLs(t *testing.T, ttlSec, maxStaleSec int) {
	t.Helper()
	origTTL, origStale := config.RBACPermissionCacheTTLSeconds, config.RBACPermissionMaxStaleSeconds
	config.RBACPermissionCacheTTLSeconds = ttlSec
	config.RBACPermissionMaxStaleSeconds = maxStaleSec
	t.Cleanup(func() {
		config.RBACPermissionCacheTTLSeconds = origTTL
		config.RBACPermissionMaxStaleSeconds = origStale
	})
}

func TestGetUserPermissions_CacheHitAvoidsRefetch(t *testing.T) {
	resetRBACCacheForTest(t)
	withTTLs(t, 300, 1800)

	calls := 0
	fetchFunc = func(ctx context.Context, userID int64) ([]string, map[string]bool, error) {
		calls++
		return []string{"RULE_MANAGER"}, map[string]bool{"rules:publish": true}, nil
	}

	for i := 0; i < 3; i++ {
		roles, perms, err := GetUserPermissions(context.Background(), 1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(roles) != 1 || !perms["rules:publish"] {
			t.Fatalf("unexpected result: roles=%v perms=%v", roles, perms)
		}
	}
	if calls != 1 {
		t.Fatalf("expected exactly 1 DB fetch (cached after), got %d", calls)
	}
}

func TestGetUserPermissions_RefetchesAfterTTLExpiry(t *testing.T) {
	resetRBACCacheForTest(t)
	withTTLs(t, 0, 1800) // TTL of 0 -> every call is a miss

	calls := 0
	fetchFunc = func(ctx context.Context, userID int64) ([]string, map[string]bool, error) {
		calls++
		return []string{"READ_ONLY_ANALYST"}, map[string]bool{"rules:read": true}, nil
	}

	GetUserPermissions(context.Background(), 1)
	time.Sleep(2 * time.Millisecond)
	GetUserPermissions(context.Background(), 1)

	if calls < 2 {
		t.Fatalf("expected refetch after TTL expiry, got %d calls", calls)
	}
}

func TestGetUserPermissions_ServesStaleWithinGraceWindowOnTransientError(t *testing.T) {
	resetRBACCacheForTest(t)
	withTTLs(t, 0, 1800) // TTL 0 (always stale-by-TTL), max-stale generous

	// First call succeeds and populates the cache.
	fetchFunc = func(ctx context.Context, userID int64) ([]string, map[string]bool, error) {
		return []string{"RULE_MANAGER"}, map[string]bool{"rules:publish": true}, nil
	}
	roles, perms, err := GetUserPermissions(context.Background(), 1)
	if err != nil || !perms["rules:publish"] {
		t.Fatalf("setup call failed: roles=%v perms=%v err=%v", roles, perms, err)
	}

	// Second call: DB is now "down" (transient error) — must serve the stale
	// cached entry rather than failing, since we're well within max-stale.
	fetchFunc = func(ctx context.Context, userID int64) ([]string, map[string]bool, error) {
		return nil, nil, errors.New("connection refused")
	}
	roles, perms, err = GetUserPermissions(context.Background(), 1)
	if err != nil {
		t.Fatalf("expected stale cache to be served without error, got: %v", err)
	}
	if !perms["rules:publish"] {
		t.Fatalf("expected stale permissions to still include rules:publish, got %v", perms)
	}
}

func TestGetUserPermissions_FailsClosedBeyondMaxStale(t *testing.T) {
	resetRBACCacheForTest(t)
	withTTLs(t, 0, 0) // TTL 0 AND max-stale 0 -> no grace period at all

	fetchFunc = func(ctx context.Context, userID int64) ([]string, map[string]bool, error) {
		return []string{"RULE_MANAGER"}, map[string]bool{"rules:publish": true}, nil
	}
	if _, _, err := GetUserPermissions(context.Background(), 1); err != nil {
		t.Fatalf("setup call failed: %v", err)
	}

	time.Sleep(2 * time.Millisecond)
	fetchFunc = func(ctx context.Context, userID int64) ([]string, map[string]bool, error) {
		return nil, nil, errors.New("connection refused")
	}
	if _, _, err := GetUserPermissions(context.Background(), 1); err == nil {
		t.Fatal("expected a fail-closed error once past the max-stale grace window, got nil")
	}
}

func TestGetUserPermissions_UserNotFoundDoesNotServeStale(t *testing.T) {
	resetRBACCacheForTest(t)
	withTTLs(t, 0, 1800) // generous max-stale — must NOT matter for an authoritative negative result

	fetchFunc = func(ctx context.Context, userID int64) ([]string, map[string]bool, error) {
		return []string{"RULE_MANAGER"}, map[string]bool{"rules:publish": true}, nil
	}
	GetUserPermissions(context.Background(), 1)

	// User was just deleted/disabled — an authoritative negative result must
	// evict the cache and propagate the error, NOT serve the old (possibly
	// since-revoked) permission set just because it's within the stale window.
	fetchFunc = func(ctx context.Context, userID int64) ([]string, map[string]bool, error) {
		return nil, nil, ErrUserDisabled
	}
	_, _, err := GetUserPermissions(context.Background(), 1)
	if !errors.Is(err, ErrUserDisabled) {
		t.Fatalf("expected ErrUserDisabled to propagate, got %v", err)
	}

	permCacheMu.RLock()
	_, stillCached := permCache[1]
	permCacheMu.RUnlock()
	if stillCached {
		t.Fatal("expected cache entry to be evicted after an authoritative disabled/not-found result")
	}
}

func TestInvalidateUserPermissions_ForcesRefetch(t *testing.T) {
	resetRBACCacheForTest(t)
	withTTLs(t, 300, 1800) // long TTL — refetch must happen ONLY because of explicit invalidation

	calls := 0
	fetchFunc = func(ctx context.Context, userID int64) ([]string, map[string]bool, error) {
		calls++
		return []string{"RULE_MANAGER"}, map[string]bool{"rules:publish": true}, nil
	}

	GetUserPermissions(context.Background(), 1)
	InvalidateUserPermissions(1)
	GetUserPermissions(context.Background(), 1)

	if calls != 2 {
		t.Fatalf("expected invalidation to force a second fetch, got %d calls", calls)
	}
}
