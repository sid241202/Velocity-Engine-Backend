// Package migrations embeds the versioned SQL migration files so they ship
// inside the compiled binary (no separate file deployment step needed) while
// still existing as plain, individually reviewable/runnable .sql files on
// disk for anyone who wants to apply them manually via a mysql client.
package migrations

import "embed"

//go:embed mysql/*.sql
var MySQLFS embed.FS
