package storage

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/rs/zerolog"
)

// s3ListStub answers ListObjectsV2 from a sorted key set, pageSize entries
// (contents plus common prefixes) per page, and records every request.
type s3ListStub struct {
	keys     []string
	pageSize int
	mu       sync.Mutex
	requests []string // "prefix|delimiter|token"
}

func (s *s3ListStub) handle(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	prefix, delim, token := q.Get("prefix"), q.Get("delimiter"), q.Get("continuation-token")
	s.mu.Lock()
	s.requests = append(s.requests, prefix+"|"+delim+"|"+token)
	s.mu.Unlock()

	type entry struct {
		name   string
		common bool
	}
	var entries []entry
	seen := map[string]bool{}
	for _, k := range s.keys {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		if delim != "" {
			if i := strings.Index(k[len(prefix):], delim); i >= 0 {
				cp := k[:len(prefix)+i+len(delim)]
				if !seen[cp] {
					seen[cp] = true
					entries = append(entries, entry{cp, true})
				}
				continue
			}
		}
		entries = append(entries, entry{k, false})
	}
	start := 0
	if token != "" {
		start, _ = strconv.Atoi(token)
	}
	end := min(start+s.pageSize, len(entries))
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>b</Name>`)
	fmt.Fprintf(&b, "<Prefix>%s</Prefix><KeyCount>%d</KeyCount><MaxKeys>%d</MaxKeys>", prefix, end-start, s.pageSize)
	if delim != "" {
		fmt.Fprintf(&b, "<Delimiter>%s</Delimiter>", delim)
	}
	for _, e := range entries[start:end] {
		if e.common {
			fmt.Fprintf(&b, "<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>", e.name)
		} else {
			fmt.Fprintf(&b, "<Contents><Key>%s</Key><Size>1</Size></Contents>", e.name)
		}
	}
	if end < len(entries) {
		fmt.Fprintf(&b, "<IsTruncated>true</IsTruncated><NextContinuationToken>%d</NextContinuationToken>", end)
	} else {
		b.WriteString("<IsTruncated>false</IsTruncated>")
	}
	b.WriteString("</ListBucketResult>")
	w.Header().Set("Content-Type", "application/xml")
	_, _ = w.Write([]byte(b.String()))
}

func (s *s3ListStub) sent() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.requests...)
}

func newS3ListStub(t *testing.T, prefix string, pageSize int, keys ...string) (*S3Backend, *s3ListStub) {
	t.Helper()
	stub := &s3ListStub{pageSize: pageSize}
	for _, k := range keys {
		stub.keys = append(stub.keys, prefix+k)
	}
	sort.Strings(stub.keys)
	server := httptest.NewServer(http.HandlerFunc(stub.handle))
	t.Cleanup(server.Close)
	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("key", "secret", "")),
	)
	if err != nil {
		t.Fatalf("load AWS config: %v", err)
	}
	return &S3Backend{
		client: s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(server.URL)
			o.UsePathStyle = true
		}),
		bucket: "b",
		prefix: prefix,
		logger: zerolog.Nop(),
	}, stub
}

// A day's raw tail: thousands of objects in hour folders, a few compacted
// files directly in the day. One delimiter listing answers which hour folders
// exist and which files sit directly in the day, without paging through the
// objects below the hours.
func TestS3ListLevel(t *testing.T) {
	keys := []string{
		"db/m/2026/11/10/m_reorg_00.parquet",
		"db/m/2026/11/10/m_reorg_01.parquet",
		"db/m/2026/11/10/",                  // directory marker: not an object a listing returns
		"db/m/2026/11/10/.hidden/x.parquet", // dot folder: dropped, like ListDirectories
		"db/m/2026/11/11/00/next.parquet",
	}
	for h := 0; h < 14; h++ {
		for i := 0; i < 50; i++ {
			keys = append(keys, fmt.Sprintf("db/m/2026/11/10/%02d/raw_%03d.parquet", h, i))
		}
	}
	for _, prefix := range []string{"", "pfx/"} {
		t.Run("prefix="+prefix, func(t *testing.T) {
			b, stub := newS3ListStub(t, prefix, 1000, keys...)
			dirs, files, err := b.ListLevel(context.Background(), "db/m/2026/11/10/")
			if err != nil {
				t.Fatal(err)
			}
			var wantDirs []string
			for h := 0; h < 14; h++ {
				wantDirs = append(wantDirs, fmt.Sprintf("%02d", h))
			}
			wantFiles := []string{"db/m/2026/11/10/m_reorg_00.parquet", "db/m/2026/11/10/m_reorg_01.parquet"}
			sort.Strings(dirs)
			if !reflect.DeepEqual(dirs, wantDirs) || !reflect.DeepEqual(files, wantFiles) {
				t.Fatalf("dirs %v files %v, want %v %v", dirs, files, wantDirs, wantFiles)
			}
			if got := stub.sent(); len(got) != 1 || !strings.HasSuffix(got[0], "|/|") {
				t.Fatalf("want one delimiter request, sent %v", got)
			}
		})
	}

	t.Run("paginated, prefix without slash", func(t *testing.T) {
		b, stub := newS3ListStub(t, "", 3, keys...)
		dirs, files, err := b.ListLevel(context.Background(), "db/m/2026/11/10")
		if err != nil {
			t.Fatal(err)
		}
		if len(dirs) != 14 || len(files) != 2 {
			t.Fatalf("dirs %v files %v", dirs, files)
		}
		if n := len(stub.sent()); n < 2 {
			t.Fatalf("expected several pages, sent %d", n)
		}
	})
}

// A month listing whose newest days hold the raw tail stops paging once it
// reaches them: keys at or after end are neither returned nor fetched past
// the page that holds the first of them.
func TestS3ListBefore(t *testing.T) {
	var keys []string
	for d := 1; d <= 8; d++ {
		for i := 0; i < 3; i++ {
			keys = append(keys, fmt.Sprintf("db/m/2026/11/%02d/m_daily_%d.parquet", d, i))
		}
	}
	keys = append(keys, "db/m/2026/11/05/03/late.parquet", "db/m/2026/11/05/03/")
	for i := 0; i < 40; i++ {
		keys = append(keys, fmt.Sprintf("db/m/2026/11/10/%02d/raw.parquet", i%24)+strconv.Itoa(i))
	}
	keys = append(keys, "db/m/2026/12/01/m_daily_0.parquet")
	for _, prefix := range []string{"", "pfx/"} {
		t.Run("prefix="+prefix, func(t *testing.T) {
			b, stub := newS3ListStub(t, prefix, 10, keys...)
			got, err := b.ListBefore(context.Background(), "db/m/2026/11/", "db/m/2026/11/09/")
			if err != nil {
				t.Fatal(err)
			}
			var want []string
			for _, k := range keys {
				if strings.HasPrefix(k, "db/m/2026/11/") && k < "db/m/2026/11/09/" && !strings.HasSuffix(k, "/") {
					want = append(want, k)
				}
			}
			sort.Strings(want)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("got %v\nwant %v", got, want)
			}
			// 25 keys before end (and the marker) fill three pages of 10; the
			// third holds the first key at end. No fourth page is fetched.
			if n := len(stub.sent()); n != 3 {
				t.Fatalf("sent %d requests, want 3: %v", n, stub.sent())
			}
		})
	}
	t.Run("empty end lists everything", func(t *testing.T) {
		b, _ := newS3ListStub(t, "", 10, keys...)
		all, err := b.ListBefore(context.Background(), "db/m/2026/11/", "")
		if err != nil {
			t.Fatal(err)
		}
		ref, err := b.List(context.Background(), "db/m/2026/11/")
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(all, ref) {
			t.Fatalf("ListBefore(end=\"\") = %d keys, List = %d", len(all), len(ref))
		}
	})
}

// The resilient wrapper prod puts around S3 keeps both capabilities.
func TestResilientDelegatesLevelListings(t *testing.T) {
	b, _ := newS3ListStub(t, "", 1000, "db/m/2026/11/10/05/a.parquet", "db/m/2026/11/10/d.parquet", "db/m/2026/11/11/x.parquet")
	r := NewResilientBackend(b, DefaultResilientConfig(), zerolog.Nop())
	dirs, files, err := r.ListLevel(context.Background(), "db/m/2026/11/10/")
	if err != nil || !reflect.DeepEqual(dirs, []string{"05"}) || !reflect.DeepEqual(files, []string{"db/m/2026/11/10/d.parquet"}) {
		t.Fatalf("ListLevel = %v %v %v", dirs, files, err)
	}
	keys, err := r.ListBefore(context.Background(), "db/m/2026/11/", "db/m/2026/11/11/")
	if err != nil || !reflect.DeepEqual(keys, []string{"db/m/2026/11/10/05/a.parquet", "db/m/2026/11/10/d.parquet"}) {
		t.Fatalf("ListBefore = %v %v", keys, err)
	}
}
