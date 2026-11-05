package producer

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestTreePresent(t *testing.T) {
	root := filepath.Join(t.TempDir(), "tree")
	if ok, err := treePresent(root); err != nil || ok {
		t.Fatalf("absent tree: present=%v err=%v", ok, err)
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if ok, err := treePresent(root); err != nil || ok {
		t.Fatalf("empty dir: present=%v err=%v (want false, nil)", ok, err)
	}
	if err := os.WriteFile(filepath.Join(root, "x"), []byte("y"), 0o644); err != nil {
		t.Fatal(err)
	}
	if ok, err := treePresent(root); err != nil || !ok {
		t.Fatalf("dir with file: present=%v err=%v (want true, nil)", ok, err)
	}
}

func TestAdoptTree(t *testing.T) {
	// Sole directory under a foreign name → renamed into place.
	dataDir := t.TempDir()
	foreign := filepath.Join(dataDir, "maildir")
	if err := os.MkdirAll(filepath.Join(foreign, "user-a"), 0o755); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(dataDir, "enron_mail_20150507")
	if err := adoptTree(dataDir, root, quietLog()); err != nil {
		t.Fatalf("adoptTree: %v", err)
	}
	if st, err := os.Stat(root); err != nil || !st.IsDir() {
		t.Fatalf("expected tree at %s (err=%v)", root, err)
	}
	if _, err := os.Stat(foreign); !os.IsNotExist(err) {
		t.Fatalf("foreign name should be gone (err=%v)", err)
	}

	// No directories at all → unresolvable, must error.
	empty := t.TempDir()
	if err := adoptTree(empty, filepath.Join(empty, "enron_mail_20150507"), quietLog()); err == nil {
		t.Fatal("adoptTree should fail when no directory exists")
	}

	// Multiple candidate directories → ambiguous, must error.
	amb := t.TempDir()
	if err := os.MkdirAll(filepath.Join(amb, "a"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(amb, "b"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := adoptTree(amb, filepath.Join(amb, "enron_mail_20150507"), quietLog()); err == nil {
		t.Fatal("adoptTree should fail when the layout is ambiguous")
	}
}

func TestSafeJoinRejectsEscape(t *testing.T) {
	const dest = "/data"
	valid := []string{
		"enron_mail_20150507",
		"enron_mail_20150507/1/all/100",
		"a/b/c",
	}
	for _, name := range valid {
		if _, ok := safeJoin(dest, name); !ok {
			t.Fatalf("safeJoin(%q) rejected a safe name", name)
		}
	}
	invalid := []string{
		"",            // empty
		".",           // the dir itself
		"/etc/passwd", // absolute
		"../evil",     // parent escape
		"..",
		"a/../../evil",   // mid-path escape
		"a/..",           // collapses back to dest
		"a/b/../../../c", // deep escape
	}
	for _, name := range invalid {
		if _, ok := safeJoin(dest, name); ok {
			t.Fatalf("safeJoin(%q) accepted an escaping name", name)
		}
	}
}

func writeTarGz(t *testing.T, path string, entries map[string]tarEntry) {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tr := tar.NewWriter(gz)
	for name, e := range entries {
		hdr := &tar.Header{Name: name, Typeflag: e.Typeflag, Size: int64(len(e.Body))}
		if e.Typeflag == tar.TypeSymlink {
			hdr.Linkname = e.Body
		}
		if err := tr.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if e.Typeflag == tar.TypeReg {
			if _, err := io.WriteString(tr, e.Body); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
}

type tarEntry struct {
	Typeflag uint8
	Body     string
}

func TestExtractTarGzRoundTrip(t *testing.T) {
	tmp := t.TempDir()
	tarPath := filepath.Join(tmp, "enron.tar.gz")
	writeTarGz(t, tarPath, map[string]tarEntry{
		"enron_mail_20150507":           {Typeflag: tar.TypeDir},
		"enron_mail_20150507/1":         {Typeflag: tar.TypeDir},
		"enron_mail_20150507/1/all":     {Typeflag: tar.TypeDir},
		"enron_mail_20150507/1/all/100": {Typeflag: tar.TypeReg, Body: "From: a@b\n\nbody"},
		"enron_mail_20150507/1/link":    {Typeflag: tar.TypeSymlink, Body: "all/100"},
		"legit.txt":                     {Typeflag: tar.TypeReg, Body: "fine"},
		// The hostile entries: none of them may materialize.
		"../escape.txt": {Typeflag: tar.TypeReg, Body: "bad"},
		"/etc/evil":     {Typeflag: tar.TypeReg, Body: "bad"},
	})

	destDir := filepath.Join(t.TempDir(), "data")
	if err := os.MkdirAll(destDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := extractTarGz(tarPath, destDir, quietLog()); err != nil {
		t.Fatalf("extractTarGz: %v", err)
	}

	// Good entries landed (including the plain in-tree file).
	for _, want := range []struct{ path, body string }{
		{"enron_mail_20150507/1/all/100", "From: a@b\n\nbody"},
		{"legit.txt", "fine"},
	} {
		full := filepath.Join(destDir, filepath.FromSlash(want.path))
		got, err := os.ReadFile(full)
		if err != nil {
			t.Fatalf("expected file at %s: %v", full, err)
		}
		if string(got) != want.body {
			t.Fatalf("content of %s = %q, want %q", want.path, got, want.body)
		}
	}
	link := filepath.Join(destDir, "enron_mail_20150507/1/link")
	if st, err := os.Lstat(link); err != nil || st.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("expected symlink at %s (mode=%v err=%v)", link, st, err)
	}

	// Hostile entries did not.
	for _, bad := range []string{
		filepath.Join(filepath.Dir(destDir), "escape.txt"),
		"/etc/evil",
	} {
		if _, err := os.Lstat(bad); err == nil {
			t.Fatalf("unsafe entry materialized at %s", bad)
		}
	}
}
