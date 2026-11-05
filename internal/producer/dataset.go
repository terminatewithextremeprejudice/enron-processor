package producer

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// The assignment pins the dataset release, so the fetcher pins it too —
// same URL and tree name as scripts/download_dataset.sh, only implemented
// in Go: the service image is distroless (no shell, no tar binary), so a
// script call is not an option inside the container.
const (
	datasetNameCanonical = "enron_mail_20150507"
	datasetTarballName   = datasetNameCanonical + ".tar.gz"
	datasetURL           = "https://www.cs.cmu.edu/~enron/" + datasetTarballName
)

// datasetRoot is where the unpacked tree is expected to live:
// <data_dir>/<dataset_name>.
func datasetRoot(cfg Config) string {
	return filepath.Join(cfg.DataDir, cfg.DatasetName)
}

// ensureDataset returns the unpacked dataset root, downloading and
// extracting it first when the tree is missing or empty. Idempotent: an
// existing non-empty tree is used as-is, so restarts and re-runs are free,
// and an interrupted fetch never looks complete to the next run (the
// tarball lands in a .part file that is only renamed on success).
func ensureDataset(ctx context.Context, cfg Config, log *slog.Logger) (string, error) {
	root := datasetRoot(cfg)

	present, err := treePresent(root)
	if err != nil {
		return "", fmt.Errorf("stat dataset tree: %w", err)
	}
	if present {
		log.Debug("dataset already unpacked", "root", root)
		return root, nil
	}

	log.Info("dataset missing, downloading", "url", datasetURL, "dest", root)
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return "", fmt.Errorf("create data dir %q: %w", cfg.DataDir, err)
	}
	tarPath := filepath.Join(cfg.DataDir, datasetTarballName)
	if err := downloadTarball(ctx, datasetURL, tarPath, log); err != nil {
		return "", fmt.Errorf("download dataset: %w", err)
	}
	if err := extractTarGz(tarPath, cfg.DataDir, log); err != nil {
		// Keep the tarball so the retry does not pay the download again.
		return "", fmt.Errorf("extract dataset: %w", err)
	}
	if err := os.Remove(tarPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("remove downloaded tarball: %w", err)
	}

	present, err = treePresent(root)
	if err != nil {
		return "", fmt.Errorf("stat dataset tree: %w", err)
	}
	if !present {
		// The tarball does not name its top-level directory after the
		// dataset (this release unpacks into "maildir/"). Normalize the
		// layout instead of assuming it: adopt the sole directory under
		// dataDir into the expected place, so the root contract
		// <data_dir>/<dataset_name> always holds.
		if err := adoptTree(cfg.DataDir, root, log); err != nil {
			return "", fmt.Errorf("adopt dataset tree: %w", err)
		}
		present, err = treePresent(root)
		if err != nil {
			return "", fmt.Errorf("stat dataset tree: %w", err)
		}
		if !present {
			return "", fmt.Errorf("dataset tree %q still missing after extraction", root)
		}
	}
	log.Info("dataset ready", "root", root)
	return root, nil
}

// adoptTree renames the sole directory directly under dataDir to root
// (which must be dataDir/<dataset_name>) when the tarball packed its
// top-level directory under a different name. Zero or multiple candidate
// directories are an unresolvable layout and an error — not a guess.
func adoptTree(dataDir, root string, log *slog.Logger) error {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return err
	}
	var dirs []string
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, e.Name())
		}
	}
	want := filepath.Base(root)
	if len(dirs) == 1 && dirs[0] != want {
		src := filepath.Join(dataDir, dirs[0])
		log.Info("adopting extracted tree into place", "from", src, "to", root)
		if err := os.Rename(src, root); err != nil {
			return fmt.Errorf("rename %s to %s: %w", src, root, err)
		}
		return nil
	}
	return fmt.Errorf("cannot locate the dataset tree under %q after extraction (found dirs: %v)", dataDir, dirs)
}

// treePresent: root is a directory containing at least one entry. An empty
// directory means a botched previous extraction, which a re-fetch must
// repair.
func treePresent(root string) (bool, error) {
	st, err := os.Stat(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if !st.IsDir() {
		return false, nil
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return false, err
	}
	return len(entries) > 0, nil
}

const (
	// downloadLogInterval throttles the in-flight progress line.
	downloadLogInterval = 5 * time.Second
)

// progressLog wraps a read source and emits a log line every so often so a
// multi-hundred-MiB fetch stays observable without flooding the log.
type progressLog struct {
	in      io.Reader
	log     *slog.Logger
	since   time.Time
	written int64
	lastLog int64
}

func (p *progressLog) Read(b []byte) (int, error) {
	n, err := p.in.Read(b)
	p.written += int64(n)
	if now := time.Now(); now.Sub(p.since) >= downloadLogInterval && p.written-p.lastLog >= 2<<20 {
		p.log.Info("download in progress", "bytes", p.written)
		p.since = now
		p.lastLog = p.written
	}
	return n, err
}

func downloadTarball(ctx context.Context, url, dest string, log *slog.Logger) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: unexpected status %s", url, resp.Status)
	}

	// Download into a .part sibling and rename on success: a half-fetched
	// tarball must never masquerade as a complete one to the next run.
	tmp := dest + ".part"
	out, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("create %s: %w", tmp, err)
	}
	pl := &progressLog{in: resp.Body, log: log, since: time.Now()}
	n, err := io.Copy(out, pl)
	if err == nil {
		err = out.Sync()
	}
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmp)
		return fmt.Errorf("download to %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, dest); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("move %s into place: %w", tmp, err)
	}
	log.Info("dataset tarball downloaded", "bytes", n, "dest", dest)
	return nil
}

// extractTarGz unpacks a .tar.gz under destDir — the same semantics as
// `tar -xzf`, without the shell. Absolute paths, ".." segments, and other
// entries that would land outside destDir are skipped and logged: this is
// a public tarball we do not control.
func extractTarGz(tarPath, destDir string, log *slog.Logger) error {
	in, err := os.Open(tarPath)
	if err != nil {
		return err
	}
	defer in.Close()

	gz, err := gzip.NewReader(in)
	if err != nil {
		return fmt.Errorf("open gzip stream: %w", err)
	}
	defer gz.Close()

	tr := tar.NewReader(gz)
	dest := filepath.Clean(destDir)
	var files, dirs, skipped int64

	for {
		hdr, nextErr := tr.Next()
		if errors.Is(nextErr, io.EOF) {
			break
		}
		if nextErr != nil {
			return fmt.Errorf("read tar entry: %w", nextErr)
		}

		target, ok := safeJoin(dest, hdr.Name)
		if !ok {
			skipped++
			log.Warn("skipping unsafe tar entry", "name", hdr.Name)
			continue
		}

		switch {
		case hdr.Typeflag == tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return fmt.Errorf("mkdir %s: %w", target, err)
			}
			dirs++

		case hdr.Typeflag == tar.TypeReg || hdr.Typeflag == '\x00' || hdr.Typeflag == tar.TypeLink:
			// Regular files; hardlinks are materialized as byte copies —
			// the walk never follows links, and per-path dedupe does not
			// care either way, so plain copies are lossless for ingest.
			if err := writeTarFile(target, tr); err != nil {
				return err
			}
			files++

		case hdr.Typeflag == tar.TypeSymlink:
			// Created where it is declared and never followed by the
			// walk (WalkDir does not descend through symlinks), so a
			// symlink loop cannot duplicate mail.
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return fmt.Errorf("mkdir for symlink %s: %w", target, err)
			}
			os.Remove(target) // idempotent re-extraction over a partial tree
			if err := os.Symlink(hdr.Linkname, target); err != nil {
				skipped++
				log.Warn("skipping symlink", "name", hdr.Name, "err", err)
			}

		default:
			skipped++
			log.Debug("skipping tar entry type", "name", hdr.Name, "type", string(hdr.Typeflag))
		}
	}

	log.Info("extraction complete", "files", files, "dirs", dirs, "skipped", skipped)
	return nil
}

func writeTarFile(target string, src io.Reader) error {
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return fmt.Errorf("mkdir for %s: %w", target, err)
	}
	out, err := os.OpenFile(target, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("create %s: %w", target, err)
	}
	if _, err := io.Copy(out, src); err != nil {
		out.Close()
		return fmt.Errorf("write %s: %w", target, err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("close %s: %w", target, err)
	}
	return nil
}

// safeJoin resolves a tar entry name under dest and rejects anything that
// would land outside it (absolute names, ".", ".." segments, trickery).
func safeJoin(dest, name string) (string, bool) {
	if name == "" || name == "." || strings.HasPrefix(name, "/") {
		return "", false
	}
	for _, seg := range strings.Split(name, "/") {
		if seg == ".." {
			return "", false
		}
	}
	target := filepath.Join(dest, filepath.FromSlash(name))
	if target == dest || strings.HasPrefix(target, dest+string(os.PathSeparator)) {
		return target, true
	}
	return "", false
}
