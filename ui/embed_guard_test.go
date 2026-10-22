package ui

import (
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The server sends style-src 'self' with no nonce and no font-src
// exception. A bundle that injects a <style> element, writes inline style
// strings, or inlines a font as data: still builds and still passes every
// jsdom test, then fails silently in a real browser: the page loses its styles. These tests read what actually ships.

// distForbidden are strings that only appear in a bundle doing something
// the CSP blocks. Every dependency's bundle is checked too: none is
// trusted to stay clean across upgrades.
var distForbidden = []string{
	`.cssText=`,
	`.cssText =`,
	`.cssText+=`,
	`["cssText"]`,
	`['cssText']`,
	"[`cssText`]",
	`setAttribute("style"`,
	`setAttribute('style'`,
	// The minifier rewrites string literals to template literals, so the
	// built form of setAttribute("style", ...) uses backticks.
	"setAttribute(`style`",
	`data:font`,
	`data:application/font`,
	`data:application/x-font`,
}

// distCSSTextRe and distHTMLStyleRe catch spacing and operator variants a literal list
// misses: cssText += s, and an inline style attribute in the HTML with
// either quote.
var (
	distCSSTextRe   = regexp.MustCompile(`cssText\s*\+?=`)
	distHTMLStyleRe = regexp.MustCompile(`\bstyle\s*=\s*['"]`)
)

// sourceForbidden are checked in our own sources instead of dist/,
// because React DOM's <style precedence> support ships
// createElement(`style`) in every build. Searching dist/ for it would
// fail on code our views can never reach; searching src/ fails the moment
// one of our files asks for it.
var sourceForbidden = []string{
	`.cssText`,
	`createElement('style')`,
	`createElement("style")`,
	"createElement(`style`)",
	// React 19 hoists a JSX <style> into a real style element.
	`<style`,
	`setAttribute('style'`,
	`setAttribute("style"`,
	"setAttribute(`style`",
	`dangerouslySetInnerHTML`,
	`innerHTML`,
	`outerHTML`,
	`insertAdjacentHTML`,
	`document.write`,
	`createContextualFragment`,
	`DOMParser`,
}

var blockCommentRe = regexp.MustCompile(`(?s)/\*.*?\*/`)

func TestBuiltUIHasNothingTheCSPBlocks(t *testing.T) {
	root := FS()
	var hits []string
	err := fs.WalkDir(root, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		ext := path.Ext(p)
		if ext != ".js" && ext != ".css" && ext != ".html" {
			return nil
		}
		b, err := fs.ReadFile(root, p)
		if err != nil {
			return err
		}
		s := string(b)
		for _, bad := range distForbidden {
			if strings.Contains(s, bad) {
				hits = append(hits, p+": "+bad)
			}
		}
		if ext != ".css" {
			for _, m := range distCSSTextRe.FindAllString(s, -1) {
				hits = append(hits, p+": "+m)
			}
		}
		if ext == ".html" {
			for _, m := range distHTMLStyleRe.FindAllString(s, -1) {
				hits = append(hits, p+": "+m)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range hits {
		t.Errorf("CSP would block: %s", h)
	}
}

func TestSourcesAskForNothingTheCSPBlocks(t *testing.T) {
	err := filepath.WalkDir("src", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		switch filepath.Ext(p) {
		case ".ts", ".tsx", ".js", ".jsx", ".mts", ".mjs":
		default:
			return nil
		}
		// Tests may name a forbidden string to assert it never renders.
		isTest := strings.Contains(filepath.Base(p), ".test.")
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		// Comments may name a banned API to explain the ban. Block comments are blanked, keeping their newlines so
		// line numbers hold, and whole // lines are skipped; code before
		// or after a comment on the same line is still checked, so
		// "/* x */ el.innerHTML = s" fails.
		src := blockCommentRe.ReplaceAllStringFunc(string(b), func(c string) string {
			return strings.Repeat("\n", strings.Count(c, "\n"))
		})
		for i, line := range strings.Split(src, "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "//") {
				continue
			}
			if !isTest {
				for _, bad := range sourceForbidden {
					if strings.Contains(line, bad) {
						t.Errorf("%s:%d: %s", p, i+1, bad)
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// An inline <script> is blocked by script-src 'self', and a page whose
// only script is blocked renders nothing. The built index.html must load
// its code from src= alone.
func TestBuiltIndexHasNoInlineScript(t *testing.T) {
	b, err := fs.ReadFile(FS(), "index.html")
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range regexp.MustCompile(`(?is)<script\b[^>]*>`).FindAllString(string(b), -1) {
		if !regexp.MustCompile(`(?i)\bsrc\s*=`).MatchString(m) {
			t.Errorf("index.html has a script tag without src: %s", m)
		}
	}
}
