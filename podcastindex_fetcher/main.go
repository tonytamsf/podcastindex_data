package main

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"sync"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

var (
	db           *sqlx.DB
	maxConcurrent int
	wg           sync.WaitGroup
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU()) // Utilize all available CPU cores

	var err error

	// Open SQLite database with connection pooling
	db, err = sqlx.Open("sqlite3", "urls.db")
	if err != nil {
		fmt.Println("Error opening database:", err)
		return
	}
	defer db.Close()

	// Create table if not exists
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS urls (url TEXT, content TEXT)`)
	if err != nil {
		fmt.Println("Error creating table:", err)
		return
	}

	// Open file containing URLs
	file, err := os.Open("urls.txt")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	// Determine the appropriate value for maxConcurrent based on system resources
	maxConcurrent = runtime.NumCPU() * 150 // Adjust as needed

	// Create a channel to communicate between workers
	urlsChannel := make(chan string, maxConcurrent)

	// Create a semaphore for concurrency control
	semaphore := make(chan struct{}, maxConcurrent)

	// Start multiple workers to fetch URLs concurrently
	numWorkers := maxConcurrent
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(urlsChannel, semaphore)
	}

	// Read URLs from the file and send them to the channel
	go func() {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			url := scanner.Text()
			fmt.Printf("Enqueuing URL: %s\n", url)
			urlsChannel <- url
		}
		close(urlsChannel)
	}()

	// Wait for all workers to finish
	wg.Wait()
	fmt.Println("All workers have completed.")
}

func worker(urlsChannel <-chan string, semaphore chan struct{}) {
	defer wg.Done()

	for url := range urlsChannel {
		// Acquire a slot from the semaphore
		semaphore <- struct{}{}

		content, err := fetchURL(url)
		if err != nil {
			fmt.Printf("Error fetching %s: %v\n", url, err)
		} else {
			err = saveToDatabaseWithTransaction(url, content)
			if err != nil {
				fmt.Printf("Error saving to database for %s: %v\n", url, err)
			} else {
				fmt.Printf("Processed URL: %s\n", url)
			}
		}

		// Release the slot back to the semaphore
		<-semaphore
	}
}

func fetchURL(url string) (string, error) {
	response, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return "", err
	}

	return string(body), nil
}

func saveToDatabaseWithTransaction(url, content string) error {
	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec("INSERT INTO urls (url, content) VALUES (?, ?)", url, content)
	if err != nil {
		return err
	}

	return tx.Commit()
}

