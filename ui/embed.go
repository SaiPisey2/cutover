// Package ui holds the built web UI. The sources live beside this file;
// `npm run build` writes dist/, which is committed so that building the
// cutover binary needs no Node toolchain.
package ui

import (
	"embed"
	"io/fs"
)

// all: includes files whose names start with "." or "_", which a plain
// pattern would silently drop from the embed.
//
//go:embed all:dist
var Dist embed.FS

// FS returns the built UI rooted at dist/, so index.html is at its top.
func FS() fs.FS {
	sub, err := fs.Sub(Dist, "dist")
	if err != nil {
		// fs.Sub only fails on an invalid path, and "dist" is a constant.
		panic(err)
	}
	return sub
}
