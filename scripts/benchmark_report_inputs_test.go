package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func fixture(t *testing.T) (string, environment) {
	t.Helper()
	logs := t.TempDir()
	env := environment{"rustc 1.94.0", "linux", "amd64", "github-hosted:ubuntu-24.04"}
	for _, s := range suites {
		dir := filepath.Join(logs, "criterion-main-vs-pr-"+s.id)
		for _, side := range []string{"base", "pr"} {
			if err := os.MkdirAll(filepath.Join(dir, side), 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, side, "benchmarks.txt"), []byte("benchmark log"), 0644); err != nil {
				t.Fatal(err)
			}
		}
		if err := writeJSON(filepath.Join(dir, "environment.json"), env); err != nil {
			t.Fatal(err)
		}
	}
	return logs, env
}
func TestAssemble(t *testing.T) {
	logs, _ := fixture(t)
	out := filepath.Join(t.TempDir(), "manifests")
	if err := assemble(logs, out, strings.Repeat("a", 40), strings.Repeat("b", 40)); err != nil {
		t.Fatal(err)
	}
	for _, side := range []string{"base", "head"} {
		data, err := os.ReadFile(filepath.Join(out, side+".json"))
		if err != nil {
			t.Fatal(err)
		}
		var m manifest
		if err = json.Unmarshal(data, &m); err != nil {
			t.Fatal(err)
		}
		if len(m.ExpectedSuites) != 4 || len(m.Suites) != 4 {
			t.Fatal("incomplete inventory")
		}
		revision := strings.Repeat("a", 40)
		if side == "head" {
			revision = strings.Repeat("b", 40)
		}
		if m.SchemaVersion != 1 || m.Revision != revision || m.Environment != (environment{"rustc 1.94.0", "linux", "amd64", "github-hosted:ubuntu-24.04"}) || !reflect.DeepEqual(m.ExpectedSuites, []string{"client", "job", "skiff", "yson"}) {
			t.Fatal("manifest metadata differs", m)
		}
		commands := map[string]string{
			"client": "cargo bench --locked -p ytsaurus-client --bench rows -- --noplot",
			"job":    "cargo bench --locked -p ytsaurus-job --bench job_throughput -- --noplot",
			"skiff":  "cargo bench --locked -p ytsaurus-skiff --bench codec_throughput -- --noplot",
			"yson":   "cargo bench --locked -p ytsaurus-yson --bench yson_benchmark -- --noplot",
		}
		for i, s := range m.Suites {
			if s.ID != m.ExpectedSuites[i] || len(s.Files) != 1 || !filepath.IsAbs(s.Files[0]) || s.Parser != (parser{"criterion", "1"}) || s.Command != commands[s.ID] {
				t.Fatal(s)
			}
		}
	}
}
func TestInvalidInputsProduceNoManifest(t *testing.T) {
	for _, mode := range []string{"missing", "empty", "environment", "incomplete", "revision"} {
		t.Run(mode, func(t *testing.T) {
			logs, env := fixture(t)
			out := filepath.Join(t.TempDir(), "manifests")
			dir := filepath.Join(logs, "criterion-main-vs-pr-yson")
			base := strings.Repeat("a", 40)
			switch mode {
			case "missing":
				if err := os.Remove(filepath.Join(dir, "base/benchmarks.txt")); err != nil {
					t.Fatal(err)
				}
			case "empty":
				if err := os.WriteFile(filepath.Join(dir, "base/benchmarks.txt"), nil, 0644); err != nil {
					t.Fatal(err)
				}
			case "environment":
				env.Toolchain = "different"
				if err := writeJSON(filepath.Join(dir, "environment.json"), env); err != nil {
					t.Fatal(err)
				}
			case "incomplete":
				env.Toolchain = ""
				if err := writeJSON(filepath.Join(dir, "environment.json"), env); err != nil {
					t.Fatal(err)
				}
			case "revision":
				base = ""
			}
			if err := assemble(logs, out, base, strings.Repeat("b", 40)); err == nil {
				t.Fatal("invalid inputs accepted")
			}
			if _, err := os.Stat(out); !os.IsNotExist(err) {
				t.Fatal("invalid inputs wrote manifests")
			}
		})
	}
}
