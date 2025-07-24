package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	maxRetries      = 3
	retryInterval   = time.Second
	rollOutMaxDelay = 5 * time.Second
	jitterMax       = 500 * time.Millisecond
)

type result struct {
	url      string
	bytes    int64
	duration time.Duration
	err      error
}

func main() {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	listPath := flag.String("list", "warcs.txt", "Path to file containing one S3 URI or CommonCrawl URL per line")
	concurrency := flag.Int("c", 1, "Number of parallel downloads")
	region := flag.String("region", "us-east-1", "AWS region for CommonCrawl bucket")
	flag.Parse()

	if *listPath == "" {
		_, _ = fmt.Fprintln(os.Stderr, "ERROR: -list is required")
		flag.Usage()
		os.Exit(1)
	}
	urls, err := readLines(*listPath)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "ERROR reading list: %v\n", err)
		os.Exit(1)
	}

	// init AWS session & S3 client
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(*region),
	}))
	s3client := s3.New(sess)

	sem := make(chan struct{}, *concurrency)
	var wg sync.WaitGroup
	resCh := make(chan result, len(urls))
	globalStart := time.Now()

	for _, u := range urls {
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			// ← gentle roll‑out: stagger start times
			time.Sleep(time.Duration(r.Intn(int(rollOutMaxDelay))))

			sem <- struct{}{}        // acquire
			defer func() { <-sem }() // release

			bucket, key, parseErr := parseS3URI(u)
			if parseErr != nil {
				resCh <- result{url: u, err: parseErr}
				return
			}

			var (
				out   *s3.GetObjectOutput
				rerr  error
				n     int64
				dur   time.Duration
				start = time.Now()
			)

			// retry on 503 / SlowDown
			for attempt := 1; attempt <= maxRetries; attempt++ {
				req, resp := s3client.GetObjectRequest(&s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				out = resp
				rerr = req.Send()
				if rerr != nil {
					if rf, ok := rerr.(awserr.RequestFailure); ok && rf.StatusCode() == 503 {
						// ← exponential back‑off + random jitter
						delay := retryInterval*time.Duration(attempt) + time.Duration(r.Intn(int(jitterMax)))
						time.Sleep(delay)
						continue
					}
				}
				break
			}

			if rerr == nil {
				n, rerr = io.Copy(io.Discard, out.Body)
				_ = out.Body.Close()
			}
			dur = time.Since(start)

			r := result{url: u, bytes: n, duration: dur, err: rerr}

			// **PRINT IMMEDIATELY****
			if r.err != nil {
				fmt.Printf("  %s → ERROR: %v\n", r.url, r.err)
			} else {
				mb := float64(r.bytes) / (1024 * 1024)
				sp := mb / r.duration.Seconds()
				fmt.Printf("  %s → %.2f MB in %.2fs (%.2f MB/s)\n",
					r.url, mb, r.duration.Seconds(), sp)
			}

			// still send to channel for your summary
			resCh <- r
		}(u)
	}

	wg.Wait()
	close(resCh)

	// report
	var totalBytes int64
	var totalDur time.Duration
	success := 0

	fmt.Println("Per-file results:")
	for r := range resCh {
		if r.err != nil {
			fmt.Printf("  %s → ERROR: %v\n", r.url, r.err)
			continue
		}
		mb := float64(r.bytes) / (1024 * 1024)
		sp := mb / r.duration.Seconds()
		fmt.Printf("  %s → %.2f MB in %.2fs (%.2f MB/s)\n",
			r.url, mb, r.duration.Seconds(), sp)
		totalBytes += r.bytes
		totalDur += r.duration
		success++
	}

	wall := time.Since(globalStart)
	overallMB := float64(totalBytes) / (1024 * 1024)
	overallSp := overallMB / wall.Seconds()

	fmt.Println("\nSummary:")
	fmt.Printf("  Success:      %d/%d files\n", success, len(urls))
	fmt.Printf("  Wall time:    %.2fs\n", wall.Seconds())
	if success > 0 {
		avgPerFile := (overallMB / float64(success)) / (totalDur.Seconds() / float64(success))
		fmt.Printf("  Avg per-file: %.2f MB/s\n", avgPerFile)
	}
	fmt.Printf("  Overall:      %.2f MB/s\n", overallSp)
}

func readLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)

	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		t := strings.TrimSpace(scanner.Text())
		if t != "" {
			lines = append(lines, t)
		}
	}
	return lines, scanner.Err()
}

// parseS3URI accepts either an s3://bucket/key or a CommonCrawl HTTPS URL
func parseS3URI(uri string) (bucket, key string, err error) {
	if strings.HasPrefix(uri, "s3://") {
		rest := strings.TrimPrefix(uri, "s3://")
		parts := strings.SplitN(rest, "/", 2)
		if len(parts) != 2 {
			return "", "", fmt.Errorf("invalid S3 URI: %s", uri)
		}
		return parts[0], parts[1], nil
	}
	const ccPrefix = "https://data.commoncrawl.org/"
	if strings.HasPrefix(uri, ccPrefix) {
		k := strings.TrimPrefix(uri, ccPrefix)
		if strings.HasPrefix(k, "/") {
			k = k[1:]
		}
		return "commoncrawl", k, nil
	}
	return "", "", fmt.Errorf("unsupported URI: %s", uri)
}
