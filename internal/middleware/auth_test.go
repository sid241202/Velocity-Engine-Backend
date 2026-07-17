package middleware

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func init() {
	gin.SetMode(gin.TestMode)
}

// wso2DepsForTest wires fake WSO2TokenValidator/ExternalSubjectResolver
// functions for the duration of a test and returns a func that clears them —
// mirrors the real SetWSO2Dependencies call cmd/server makes at startup,
// but with fakes so these tests need neither a real WSO2 tenant nor MySQL.
func wso2DepsForTest(validator WSO2TokenValidator, resolver ExternalSubjectResolver) func() {
	SetWSO2Dependencies(validator, resolver)
	return func() { SetWSO2Dependencies(nil, nil) }
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

func TestIdentityMiddleware_ValidTokenResolvesUser(t *testing.T) {
	defer wso2DepsForTest(
		func(ctx context.Context, rawToken string) (string, error) { return "wso2-subject-42", nil },
		func(ctx context.Context, sub string) (int64, string, error) { return 42, "ACTIVE", nil },
	)()

	r := gin.New()
	r.GET("/whoami", IdentityMiddleware(), func(c *gin.Context) {
		uid, _ := c.Get(ContextKeyUserID)
		c.JSON(http.StatusOK, gin.H{"user_id": uid})
	})

	req := httptest.NewRequest(http.MethodGet, "/whoami", nil)
	req.Header.Set("Authorization", "Bearer some.jwt.token")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d (body: %s)", w.Code, w.Body.String())
	}
}

func TestIdentityMiddleware_MissingBearerHeaderRejected(t *testing.T) {
	defer wso2DepsForTest(
		func(ctx context.Context, rawToken string) (string, error) {
			t.Fatal("validator must not be called without a bearer header")
			return "", nil
		},
		func(ctx context.Context, sub string) (int64, string, error) {
			t.Fatal("resolver must not be called without a bearer header")
			return 0, "", nil
		},
	)()

	r := gin.New()
	r.GET("/whoami", IdentityMiddleware(), func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequest(http.MethodGet, "/whoami", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d (body: %s)", w.Code, w.Body.String())
	}
}

func TestIdentityMiddleware_InvalidTokenRejected(t *testing.T) {
	defer wso2DepsForTest(
		func(ctx context.Context, rawToken string) (string, error) {
			return "", errors.New("signature verification failed")
		},
		func(ctx context.Context, sub string) (int64, string, error) {
			t.Fatal("resolver must not be called when token validation fails")
			return 0, "", nil
		},
	)()

	r := gin.New()
	r.GET("/whoami", IdentityMiddleware(), func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequest(http.MethodGet, "/whoami", nil)
	req.Header.Set("Authorization", "Bearer forged.token.here")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d (body: %s)", w.Code, w.Body.String())
	}
}

func TestIdentityMiddleware_UnprovisionedSubjectRejected(t *testing.T) {
	defer wso2DepsForTest(
		func(ctx context.Context, rawToken string) (string, error) { return "never-seen-before", nil },
		func(ctx context.Context, sub string) (int64, string, error) { return 0, "", ErrIdentityNotFound },
	)()

	r := gin.New()
	r.GET("/whoami", IdentityMiddleware(), func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequest(http.MethodGet, "/whoami", nil)
	req.Header.Set("Authorization", "Bearer some.jwt.token")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d (body: %s)", w.Code, w.Body.String())
	}
}

func TestIdentityMiddleware_DisabledUserRejected(t *testing.T) {
	defer wso2DepsForTest(
		func(ctx context.Context, rawToken string) (string, error) { return "wso2-subject-42", nil },
		func(ctx context.Context, sub string) (int64, string, error) { return 42, "DISABLED", nil },
	)()

	r := gin.New()
	r.GET("/whoami", IdentityMiddleware(), func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequest(http.MethodGet, "/whoami", nil)
	req.Header.Set("Authorization", "Bearer some.jwt.token")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d (body: %s)", w.Code, w.Body.String())
	}
}

func TestIdentityMiddleware_NotConfiguredWithoutDependencies(t *testing.T) {
	defer wso2DepsForTest(nil, nil)()

	r := gin.New()
	r.GET("/whoami", IdentityMiddleware(), func(c *gin.Context) { c.Status(http.StatusOK) })

	req := httptest.NewRequest(http.MethodGet, "/whoami", nil)
	req.Header.Set("Authorization", "Bearer some.jwt.token")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501, got %d (body: %s)", w.Code, w.Body.String())
	}
}
