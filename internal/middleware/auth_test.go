package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"velocity-engine-control-plane-backend-go/internal/config"

	"github.com/gin-gonic/gin"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// devModeForTest sets config.AuthDevMode for the duration of a test and
// returns a func that restores the prior value — config.AuthDevMode is a
// mutable package-level var (read directly by IdentityMiddleware), so tests
// toggle it directly rather than needing a separate injectable flag.
func devModeForTest(enabled bool) func() {
	prev := config.AuthDevMode
	config.AuthDevMode = enabled
	return func() { config.AuthDevMode = prev }
}

func newTestRouter(resolver PermissionResolver, resource, action string, setUserID interface{}) *gin.Engine {
	r := gin.New()
	authMW := NewAuthMiddleware(resolver)
	r.GET("/protected", func(c *gin.Context) {
		if setUserID != nil {
			c.Set(ContextKeyUserID, setUserID)
		}
		c.Next()
	}, authMW.RequirePermission(resource, action), func(c *gin.Context) {
		c.Status(http.StatusOK)
	})
	return r
}

func TestRequirePermission_AllowsWhenPermissionPresent(t *testing.T) {
	resolver := func(ctx context.Context, userID int64) ([]string, map[string]bool, error) {
		return []string{"RULE_MANAGER"}, map[string]bool{"rules:publish": true}, nil
	}
	r := newTestRouter(resolver, "rules", "publish", int64(42))

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
}

func TestRequirePermission_DeniesWhenPermissionMissing(t *testing.T) {
	resolver := func(ctx context.Context, userID int64) ([]string, map[string]bool, error) {
		return []string{"READ_ONLY_ANALYST"}, map[string]bool{"rules:read": true}, nil
	}
	r := newTestRouter(resolver, "rules", "publish", int64(42))

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d (body: %s)", w.Code, w.Body.String())
	}
}

func TestRequirePermission_UnauthorizedWithoutIdentity(t *testing.T) {
	resolver := func(ctx context.Context, userID int64) ([]string, map[string]bool, error) {
		t.Fatal("resolver must not be called when no identity is set")
		return nil, nil, nil
	}
	r := newTestRouter(resolver, "rules", "publish", nil)

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d (body: %s)", w.Code, w.Body.String())
	}
}

func TestRequirePermission_ServiceUnavailableOnResolverError(t *testing.T) {
	resolver := func(ctx context.Context, userID int64) ([]string, map[string]bool, error) {
		return nil, nil, errors.New("mysql unavailable")
	}
	r := newTestRouter(resolver, "rules", "publish", int64(42))

	req := httptest.NewRequest(http.MethodGet, "/protected", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d (body: %s)", w.Code, w.Body.String())
	}
}

func TestIdentityMiddleware_DevModeResolvesHeader(t *testing.T) {
	origDevMode := devModeForTest(true)
	defer origDevMode()

	r := gin.New()
	r.GET("/whoami", IdentityMiddleware(), func(c *gin.Context) {
		uid, _ := c.Get(ContextKeyUserID)
		c.JSON(http.StatusOK, gin.H{"user_id": uid})
	})

	req := httptest.NewRequest(http.MethodGet, "/whoami", nil)
	req.Header.Set("X-Debug-User-Id", "7")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
}

func TestIdentityMiddleware_MissingHeaderRejected(t *testing.T) {
	origDevMode := devModeForTest(true)
	defer origDevMode()

	r := gin.New()
	r.GET("/whoami", IdentityMiddleware(), func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequest(http.MethodGet, "/whoami", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d (body: %s)", w.Code, w.Body.String())
	}
}

func TestIdentityMiddleware_DisabledOutsideDevMode(t *testing.T) {
	origDevMode := devModeForTest(false)
	defer origDevMode()

	r := gin.New()
	r.GET("/whoami", IdentityMiddleware(), func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequest(http.MethodGet, "/whoami", nil)
	req.Header.Set("X-Debug-User-Id", "7") // present but must be ignored — dev mode is off
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d (body: %s)", w.Code, w.Body.String())
	}
}
