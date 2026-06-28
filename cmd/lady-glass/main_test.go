package main

import (
	"flag"
	"testing"
)

// TestParseInterspersed_FlagAfterPositional pins the original bug: the
// stdlib flag package stops at the first non-flag token, so a user
// running `submit ./file.pdf --mode rendered` would silently drop the
// --mode flag. The reorder shim has to lift the flag (and its value)
// before fs.Parse sees the positional.
func TestParseInterspersed_FlagAfterPositional(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	mode := fs.String("mode", "", "")
	json := fs.Bool("json", false, "")

	if err := parseInterspersed(fs, []string{"./file.pdf", "--mode", "rendered", "--json"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if *mode != "rendered" {
		t.Fatalf("mode = %q, want rendered (the flag-after-positional bug)", *mode)
	}
	if !*json {
		t.Fatal("--json should have been parsed even after a positional")
	}
	if got := fs.Arg(0); got != "./file.pdf" {
		t.Fatalf("positional = %q, want ./file.pdf", got)
	}
	if fs.NArg() != 1 {
		t.Fatalf("NArg = %d, want 1", fs.NArg())
	}
}

// TestParseInterspersed_FlagBeforePositional pins the canonical order
// still works — the fix must not regress the case stdlib already
// handled.
func TestParseInterspersed_FlagBeforePositional(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	mode := fs.String("mode", "", "")

	if err := parseInterspersed(fs, []string{"--mode", "rendered", "./file.pdf"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if *mode != "rendered" {
		t.Fatalf("mode = %q, want rendered", *mode)
	}
	if got := fs.Arg(0); got != "./file.pdf" {
		t.Fatalf("positional = %q, want ./file.pdf", got)
	}
}

// TestParseInterspersed_BoolFlagSkipsNextToken pins that a bool flag
// does not eat the following positional as its value.
func TestParseInterspersed_BoolFlagSkipsNextToken(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	verbose := fs.Bool("verbose", false, "")

	if err := parseInterspersed(fs, []string{"./file.pdf", "--verbose"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !*verbose {
		t.Fatal("--verbose not set")
	}
	if got := fs.Arg(0); got != "./file.pdf" {
		t.Fatalf("positional eaten as flag value: arg(0) = %q", got)
	}
}

// TestParseInterspersed_EqualsForm pins the --key=value single-token
// form: it must not consume the next token.
func TestParseInterspersed_EqualsForm(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	filter := fs.String("filter", "", "")

	if err := parseInterspersed(fs, []string{"./job_xxx", "--filter=foreign_currency=MYR"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if *filter != "foreign_currency=MYR" {
		t.Fatalf("filter = %q, want foreign_currency=MYR", *filter)
	}
	if got := fs.Arg(0); got != "./job_xxx" {
		t.Fatalf("positional = %q", got)
	}
}

// TestParseInterspersed_DoubleDashStopsFlagParsing pins the POSIX "--"
// convention: everything after it is positional, even if it looks like
// a flag.
func TestParseInterspersed_DoubleDashStopsFlagParsing(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	mode := fs.String("mode", "", "")

	if err := parseInterspersed(fs, []string{"--mode", "rendered", "--", "--not-a-flag", "./file.pdf"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if *mode != "rendered" {
		t.Fatalf("mode = %q", *mode)
	}
	if got := fs.Args(); len(got) != 2 || got[0] != "--not-a-flag" || got[1] != "./file.pdf" {
		t.Fatalf("positionals = %v", got)
	}
}
