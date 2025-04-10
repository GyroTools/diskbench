package main

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/urfave/cli/v2"
)

// Result stores the metrics for a single file copy operation
type Result struct {
	WorkerID     int
	IterationID  int
	FileSize     int64
	Duration     time.Duration
	TransferRate float64 // MB/s
}

func main() {
	app := &cli.App{
		Name:  "diskbench",
		Usage: "A fast, parallel disk I/O benchmarking utility",
		Description: "Measures transfer rates by copying temporary files to a target disk " +
			"and provides detailed performance metrics",
		ArgsUsage: "TARGET_PATH",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "source",
				Aliases: []string{"p"},
				Value:   "",
				Usage:   "The source directory to copy from (if not specified, the systems temp dir will be used)",
			},
			&cli.StringFlag{
				Name:    "size",
				Aliases: []string{"s"},
				Value:   "1G",
				Usage:   "Size of temporary files to create (e.g., 1G, 500M)",
			},
			&cli.IntFlag{
				Name:    "workers",
				Aliases: []string{"w"},
				Value:   1,
				Usage:   "Number of parallel copy operations",
			},
			&cli.IntFlag{
				Name:    "iterations",
				Aliases: []string{"i"},
				Value:   3,
				Usage:   "How many times to copy each file",
			},
			&cli.BoolFlag{
				Name:    "no-cleanup",
				Aliases: []string{"n"},
				Value:   false,
				Usage:   "Keep temporary files after the test completes",
			},
		},
		Action: func(c *cli.Context) error {
			if c.NArg() < 1 {
				return fmt.Errorf("error: missing target path argument")
			}

			targetPath := c.Args().Get(0)
			sourcePath := c.String("source")
			sizeStr := c.String("size")
			workers := c.Int("workers")
			iterations := c.Int("iterations")
			noCleanup := c.Bool("no-cleanup")

			// Convert size string to bytes
			size, err := parseSize(sizeStr)
			if err != nil {
				return fmt.Errorf("invalid size: %v", err)
			}

			// Validate the target path
			if _, err := os.Stat(targetPath); os.IsNotExist(err) {
				return fmt.Errorf("target path does not exist: %s", targetPath)
			}

			if sourcePath == "" {
				// Use the system's temp directory if no source is specified
				sourcePath = os.TempDir()
			}

			fmt.Printf("Starting DiskBench with the following settings:\n")
			fmt.Printf("    Source path: %s\n", sourcePath)
			fmt.Printf("    Target path: %s\n", targetPath)
			fmt.Printf("    File size: %s (%d bytes)\n", sizeStr, size)
			fmt.Printf("    Workers: %d\n", workers)
			fmt.Printf("    Iterations: %d\n", iterations)
			fmt.Printf("    No cleanup: %t\n", noCleanup)

			return runBenchmark(sourcePath, targetPath, size, workers, iterations, noCleanup)
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

// parseSize converts a human-readable size string (like "1G", "500M") to bytes
func parseSize(sizeStr string) (int64, error) {
	var multiplier int64 = 1
	var size int64

	if len(sizeStr) < 2 {
		return 0, fmt.Errorf("invalid size format")
	}

	lastChar := sizeStr[len(sizeStr)-1]
	numberPart := sizeStr[:len(sizeStr)-1]

	switch lastChar {
	case 'K', 'k':
		multiplier = 1024
	case 'M', 'm':
		multiplier = 1024 * 1024
	case 'G', 'g':
		multiplier = 1024 * 1024 * 1024
	case 'T', 't':
		multiplier = 1024 * 1024 * 1024 * 1024
	default:
		// If the last character is a digit, assume the size is in bytes
		_, err := fmt.Sscanf(sizeStr, "%d", &size)
		return size, err
	}

	_, err := fmt.Sscanf(numberPart, "%d", &size)
	if err != nil {
		return 0, err
	}
	return size * multiplier, nil
}

// runBenchmark executes the disk benchmarking test
func runBenchmark(sourcePath, targetPath string, size int64, workers int, iterations int, noCleanup bool) error {
	var srcFilePaths []string

	// try to clear the system caches before starting the benchmark (this only works under linux and with sudo)
	// err := clearCaches()
	// if err == nil {
	// 	fmt.Println("Successfully cleared system caches")
	// }

	// Create one source file per worker to prevent contention
	fmt.Printf("Creating %d temporary source files (%d bytes each)...\n", workers, size)
	for w := 1; w <= workers; w++ {
		srcFile, err := os.CreateTemp(sourcePath, fmt.Sprintf("diskbench-src-%d-*", w))
		if err != nil {
			// Clean up any files created so far
			for _, path := range srcFilePaths {
				os.Remove(path)
			}
			return fmt.Errorf("failed to create temporary source file: %v", err)
		}

		srcFilePath := srcFile.Name()
		srcFilePaths = append(srcFilePaths, srcFilePath)

		if err := createRandomFile(srcFile, size); err != nil {
			srcFile.Close()
			// Clean up any files created so far
			for _, path := range srcFilePaths {
				os.Remove(path)
			}
			return fmt.Errorf("failed to create random file: %v", err)
		}
		srcFile.Close()
	}

	// Clean up source files after we're done
	if !noCleanup {
		defer func() {
			for _, path := range srcFilePaths {
				os.Remove(path)
			}
		}()
	}

	// Create channels for results and synchronization
	resultsChan := make(chan Result, workers*iterations)
	done := make(chan struct{})
	var wg sync.WaitGroup

	// Start a separate goroutine to process and print results
	go processResults(resultsChan, done)

	fmt.Printf("\nStarting benchmark with %d workers and %d iterations each\n", workers, iterations)
	startTime := time.Now()

	// Launch workers
	for w := 1; w <= workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			// Use the source file assigned to this worker
			srcFilePath := srcFilePaths[workerID-1]

			for i := 1; i <= iterations; i++ {
				// Create a unique destination filename for this worker and iteration
				destFilePath := filepath.Join(targetPath, fmt.Sprintf("diskbench-%d-%d", workerID, i))

				// Copy the file and measure the time
				copyStart := time.Now()
				bytesCopied, err := copyFile(srcFilePath, destFilePath)
				if err != nil {
					fmt.Printf("Error in Worker #%d, iteration #%d: %v\n", workerID, i, err)
					continue
				}
				duration := time.Since(copyStart)

				// Clean up the destination file unless --no-cleanup is specified
				if !noCleanup {
					os.Remove(destFilePath)
				}

				// Calculate transfer rate in MB/s
				mbCopied := float64(bytesCopied) / (1024 * 1024)
				transferRate := mbCopied / duration.Seconds()

				// Create a result and send it to the channel
				result := Result{
					WorkerID:     workerID,
					IterationID:  i,
					FileSize:     bytesCopied,
					Duration:     duration,
					TransferRate: transferRate,
				}
				resultsChan <- result
			}
		}(w)
	}

	// Wait for all workers to complete
	wg.Wait()
	close(resultsChan)

	// Wait for the result processor to finish
	<-done

	// Calculate total execution time and print the summary
	totalDuration := time.Since(startTime)
	printSummary(targetPath, size, workers, iterations, totalDuration, resultsChan)

	return nil
}

// processResults receives results from workers and prints them in real-time
func processResults(resultsChan chan Result, done chan struct{}) {
	var results []Result

	for result := range resultsChan {
		// Store the result for the summary
		results = append(results, result)

		// Print the result
		mbCopied := float64(result.FileSize) / (1024 * 1024)
		fmt.Printf("[%s] Worker #%d completed file copy #%d: %.2f MB in %.2fs (%.2f MB/s)\n",
			time.Now().Format("2006-01-02 15:04:05"),
			result.WorkerID, result.IterationID, mbCopied,
			result.Duration.Seconds(), result.TransferRate)
	}

	// Signal that we're done processing results
	close(done)
}

// printSummary calculates and prints the summary report
func printSummary(targetPath string, size int64, workers int, iterations int, totalDuration time.Duration, resultsChan chan Result) {
	// Re-open the channel to read the results again
	var results []Result

	// Calculate summary statistics
	totalDataMB := float64(size*int64(workers*iterations)) / (1024 * 1024)
	avgTransferRate := totalDataMB / totalDuration.Seconds()

	// Find fastest and slowest copies
	var fastest, slowest Result
	fastest.TransferRate = 0
	slowest.TransferRate = float64(^uint(0) >> 1) // Max int value as float64

	// Worker stats
	workerStats := make(map[int]struct {
		total, min, max, count float64
	})

	for _, result := range results {
		// Update fastest
		if result.TransferRate > fastest.TransferRate {
			fastest = result
		}

		// Update slowest
		if result.TransferRate < slowest.TransferRate && result.TransferRate > 0 {
			slowest = result
		}

		// Update worker stats
		stats := workerStats[result.WorkerID]
		stats.total += result.TransferRate
		stats.count++
		if stats.min == 0 || result.TransferRate < stats.min {
			stats.min = result.TransferRate
		}
		if result.TransferRate > stats.max {
			stats.max = result.TransferRate
		}
		workerStats[result.WorkerID] = stats
	}

	// Print the summary report
	fmt.Printf("\n======= DISKBENCH SUMMARY REPORT =======\n")
	fmt.Printf("Target disk: %s\n", targetPath)
	fmt.Printf("Test file size: %.2f MB\n", float64(size)/(1024*1024))
	fmt.Printf("Workers: %d\n", workers)
	fmt.Printf("Iterations per worker: %d\n\n", iterations)

	fmt.Printf("Total data transferred: %.2f GB\n", totalDataMB/1024)
	fmt.Printf("Total execution time: %.2fs\n", totalDuration.Seconds())
	fmt.Printf("Average transfer rate: %.2f MB/s\n\n", avgTransferRate)

	fmt.Printf("--- Performance Details ---\n")
	if len(results) > 0 {
		fmt.Printf("Fastest copy: %.2f MB/s (Worker #%d, iteration #%d)\n",
			fastest.TransferRate, fastest.WorkerID, fastest.IterationID)
		fmt.Printf("Slowest copy: %.2f MB/s (Worker #%d, iteration #%d)\n\n",
			slowest.TransferRate, slowest.WorkerID, slowest.IterationID)
	}

	fmt.Printf("--- Per-Worker Statistics ---\n")
	for workerID, stats := range workerStats {
		avgRate := stats.total / stats.count
		fmt.Printf("Worker #%d: avg %.2f MB/s (min: %.2f, max: %.2f)\n",
			workerID, avgRate, stats.min, stats.max)
	}
}

// createRandomFile fills a file with random data up to the specified size
func createRandomFile(file *os.File, size int64) error {
	// Create a buffer with random data
	bufferSize := int64(1024 * 1024) // 1MB buffer
	if bufferSize > size {
		bufferSize = size
	}
	buffer := make([]byte, bufferSize)

	remaining := size
	for remaining > 0 {
		// Fill the buffer with random data
		rand.Read(buffer)

		// Write the buffer to the file
		writeSize := bufferSize
		if remaining < bufferSize {
			writeSize = remaining
		}

		_, err := file.Write(buffer[:writeSize])
		if err != nil {
			return err
		}

		remaining -= writeSize
	}

	return nil
}

func clearCaches() error {
	// This only works if the program is run with sudo
	f, err := os.OpenFile("/proc/sys/vm/drop_caches", os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open drop_caches: %v (may need sudo)", err)
	}
	defer f.Close()

	if _, err := f.WriteString("3\n"); err != nil {
		return fmt.Errorf("failed to clear caches: %v", err)
	}
	return nil
}

// copyFile copies a file from src to dst and returns the number of bytes copied
func copyFile(srcPath, dstPath string) (int64, error) {
	// Open the source file
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return 0, err
	}
	defer srcFile.Close()

	// Create the destination file with O_SYNC flag to ensure writes are synchronous
	dstFile, err := os.Create(dstPath)
	if err != nil {
		return 0, err
	}
	defer dstFile.Close()

	// Copy the file
	bytesWritten, err := io.Copy(dstFile, srcFile)
	if err != nil {
		return 0, err
	}

	// ensure data is written
	err = dstFile.Sync()
	if err != nil {
		return 0, err
	}

	return bytesWritten, nil
}

func init() {
	// Initialize random seed
	rand.Seed(time.Now().UnixNano())

	// Set maximum parallelism
	runtime.GOMAXPROCS(runtime.NumCPU())
}
