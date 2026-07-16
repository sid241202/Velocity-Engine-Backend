// Package services — WSO2 JWT validation. Only active when config.AuthMode
// == "wso2"; the "dev" path (internal/middleware/auth.go's X-Debug-User-Id
// shim) never calls into this file.
package services

import (
	"context"
	"fmt"
	"sync"
	"time"

	"velocity-engine-control-plane-backend-go/internal/config"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

var (
	jwksMu  sync.Mutex
	jwks    keyfunc.Keyfunc
	jwksErr error
)

// getJWKS returns the cached JWKS keyfunc, fetching it on first use.
// keyfunc.NewDefaultCtx keeps its own background refresh (on a Keyfunc's own
// schedule, and on key-ID cache-miss) — this just guards lazy first-init.
func getJWKS(ctx context.Context) (keyfunc.Keyfunc, error) {
	jwksMu.Lock()
	defer jwksMu.Unlock()

	if jwks != nil {
		return jwks, nil
	}

	kf, err := keyfunc.NewDefaultCtx(ctx, []string{config.WSO2JWKSURI})
	if err != nil {
		jwksErr = err
		return nil, err
	}

	jwks = kf
	jwksErr = nil
	return jwks, nil
}

// ValidateWSO2Token verifies a raw bearer token against WSO2's JWKS (RS256
// signature, issuer, audience, expiry — all with config.JWTClockSkewSeconds
// leeway) and returns the token's "sub" claim on success.
//
// Unlike operator360-portal-backend's decodeJWT (which base64-decodes the
// payload and never checks the signature at all — see the demo-wso2 plan's
// research notes), this performs real cryptographic verification: an
// attacker cannot forge a token by crafting an arbitrary payload.
func ValidateWSO2Token(ctx context.Context, rawToken string) (sub string, err error) {
	kf, err := getJWKS(ctx)
	if err != nil {
		return "", fmt.Errorf("JWKS unavailable: %w", err)
	}

	skew := time.Duration(config.JWTClockSkewSeconds) * time.Second
	claims := jwt.MapClaims{}
	parser := jwt.NewParser(
		jwt.WithValidMethods([]string{"RS256"}),
		jwt.WithIssuer(config.WSO2Issuer),
		jwt.WithAudience(config.WSO2Audience),
		jwt.WithLeeway(skew),
	)

	token, err := parser.ParseWithClaims(rawToken, claims, kf.Keyfunc)
	if err != nil {
		return "", fmt.Errorf("token validation failed: %w", err)
	}
	if !token.Valid {
		return "", fmt.Errorf("token is not valid")
	}

	subject, err := claims.GetSubject()
	if err != nil || subject == "" {
		return "", fmt.Errorf("token has no sub claim")
	}

	return subject, nil
}
