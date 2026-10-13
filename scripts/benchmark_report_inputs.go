// Command benchmark_report_inputs records and assembles four completed Criterion suites.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
)

type environment struct {
	Toolchain string `json:"toolchain"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	Runner    string `json:"runner"`
}
type suite struct {
	ID      string   `json:"id"`
	Parser  parser   `json:"parser"`
	Command string   `json:"command"`
	Files   []string `json:"files"`
}
type parser struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}
type manifest struct {
	SchemaVersion  int         `json:"schema_version"`
	Revision       string      `json:"revision"`
	Environment    environment `json:"environment"`
	ExpectedSuites []string    `json:"expected_suites"`
	Suites         []suite     `json:"suites"`
}

var suites = []struct{ id, pkg, bench string }{{"client", "ytsaurus-client", "rows"}, {"job", "ytsaurus-job", "job_throughput"}, {"skiff", "ytsaurus-skiff", "codec_throughput"}, {"yson", "ytsaurus-yson", "yson_benchmark"}}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func writeJSON(name string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(name, append(data, '\n'), 0644)
}
func run(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("requires record or assemble")
	}
	flags := flag.NewFlagSet(args[0], flag.ContinueOnError)
	logs := flags.String("logs", "", "downloaded suite directory")
	out := flags.String("output", "", "manifest destination")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments")
	}
	switch args[0] {
	case "record":
		data, err := exec.Command("rustc", "--version").Output()
		if err != nil {
			return err
		}
		toolchain := strings.TrimSpace(string(data))
		if toolchain == "" {
			return fmt.Errorf("empty compiler identity")
		}
		return writeJSON("environment.json", environment{toolchain, "linux", "amd64", "github-hosted:ubuntu-24.04"})
	case "assemble":
		if *logs == "" || *out == "" {
			return fmt.Errorf("assemble requires --logs and --output")
		}
		return assemble(*logs, *out, os.Getenv("BASE_SHA"), os.Getenv("HEAD_SHA"))
	default:
		return fmt.Errorf("unsupported mode %q", args[0])
	}
}
func assemble(logs, out, base, head string) error {
	if base == "" || head == "" {
		return fmt.Errorf("BASE_SHA and HEAD_SHA required")
	}
	var expected []string
	var shared environment
	bySide := map[string][]suite{"base": {}, "head": {}}
	for i, s := range suites {
		dir := filepath.Join(logs, "criterion-main-vs-pr-"+s.id)
		data, err := os.ReadFile(filepath.Join(dir, "environment.json"))
		if err != nil {
			return err
		}
		var env environment
		if err = json.Unmarshal(data, &env); err != nil {
			return err
		}
		if env.Toolchain == "" || env.OS == "" || env.Arch == "" || env.Runner == "" {
			return fmt.Errorf("incomplete environment for %s", s.id)
		}
		if i == 0 {
			shared = env
		} else if !reflect.DeepEqual(shared, env) {
			return fmt.Errorf("suite toolchains or runner environments differ")
		}
		expected = append(expected, s.id)
		for _, side := range []string{"base", "head"} {
			folder := side
			if side == "head" {
				folder = "pr"
			}
			log := filepath.Join(dir, folder, "benchmarks.txt")
			info, err := os.Stat(log)
			if err != nil || !info.Mode().IsRegular() || info.Size() == 0 {
				return fmt.Errorf("missing or empty suite log: %s", log)
			}
			physical, err := filepath.EvalSymlinks(log)
			if err != nil {
				return err
			}
			physical, err = filepath.Abs(physical)
			if err != nil {
				return err
			}
			bySide[side] = append(bySide[side], suite{s.id, parser{"criterion", "1"}, fmt.Sprintf("cargo bench --locked -p %s --bench %s -- --noplot", s.pkg, s.bench), []string{physical}})
		}
	}
	if err := os.MkdirAll(out, 0755); err != nil {
		return err
	}
	for _, side := range []string{"base", "head"} {
		revision := base
		if side == "head" {
			revision = head
		}
		if err := writeJSON(filepath.Join(out, side+".json"), manifest{1, revision, shared, expected, bySide[side]}); err != nil {
			return err
		}
	}
	return nil
}
