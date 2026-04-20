package main

import (
	"bufio"
	"compress/bzip2"
	"encoding/gob"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Page represents a Wikipedia page structure from the XML
type Page struct {
	Title    string   `xml:"title"`
	NS       int      `xml:"ns"`
	ID       int      `xml:"id"`
	Redirect Redirect `xml:"redirect"`
	Revision Revision `xml:"revision"`
}

type Redirect struct {
	Title string `xml:"title,attr"`
}

type Revision struct {
	Text string `xml:"text"`
}

// WikiProcessor handles the processing of Wikipedia dumps
type WikiProcessor struct {
	redirects      map[string]string
	adjList        map[string][]string // Changed from map[string]bool for better memory efficiency
	redirectsMutex sync.RWMutex
	adjListMutex   sync.RWMutex

	// Statistics
	processedPages int64
	redirectCount  int64
	articleCount   int64
	statsMutex     sync.RWMutex

	// Configuration
	maxPages    int
	numWorkers  int
	pageChannel chan Page
	wg          sync.WaitGroup
}

// NewWikiProcessor creates a new Wikipedia processor
func NewWikiProcessor(maxPages int) *WikiProcessor {
	numWorkers := runtime.NumCPU()
	return &WikiProcessor{
		redirects:   make(map[string]string),
		adjList:     make(map[string][]string),
		maxPages:    maxPages,
		numWorkers:  numWorkers,
		pageChannel: make(chan Page, numWorkers*2),
	}
}

// normalizeTitle handles title normalization with better Unicode support
func (wp *WikiProcessor) normalizeTitle(title string) string {
	title = strings.TrimSpace(title)
	title = strings.ReplaceAll(title, "_", " ")

	if len(title) == 0 {
		return title
	}

	// Basic capitalization - in production, use proper Unicode normalization
	return strings.ToUpper(string(title[0])) + title[1:]
}

// parseWikiTextForLinks improved link extraction
func (wp *WikiProcessor) parseWikiTextForLinks(text string) []string {
	var links []string

	// Improved regex patterns for different link types
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`\[\[([^|\[\]#]+?)(?:\|[^\[\]]*)?(?:#[^\[\]]*?)?\]\]`), // Basic links
		regexp.MustCompile(`\[\[([^|\[\]#]+?)#[^\[\]]*?\]\]`),                     // Links with fragments
	}

	for _, pattern := range patterns {
		matches := pattern.FindAllStringSubmatch(text, -1)
		for _, match := range matches {
			if len(match) > 1 {
				link := wp.normalizeTitle(match[1])
				if link != "" && !wp.isIgnoredNamespace(link) {
					links = append(links, link)
				}
			}
		}
	}

	return wp.deduplicateLinks(links)
}

// isIgnoredNamespace checks if a link should be ignored
func (wp *WikiProcessor) isIgnoredNamespace(title string) bool {
	ignorePrefixes := []string{
		"Category:", "Template:", "File:", "Image:", "Media:",
		"Special:", "User:", "Talk:", "Wikipedia:", "Help:",
	}

	for _, prefix := range ignorePrefixes {
		if strings.HasPrefix(title, prefix) {
			return true
		}
	}
	return false
}

// deduplicateLinks removes duplicate links while preserving order
func (wp *WikiProcessor) deduplicateLinks(links []string) []string {
	seen := make(map[string]bool)
	result := make([]string, 0, len(links))

	for _, link := range links {
		if !seen[link] {
			seen[link] = true
			result = append(result, link)
		}
	}

	return result
}

// processPage handles individual page processing with proper concurrency
func (wp *WikiProcessor) processPage(page Page) {
	defer wp.wg.Done()

	wp.updateStats(1, 0, 0)

	normalizedTitle := wp.normalizeTitle(page.Title)

	// Only process main namespace articles
	if page.NS != 0 || normalizedTitle == "" {
		return
	}

	// Handle redirects
	if page.Redirect.Title != "" {
		wp.redirectsMutex.Lock()
		wp.redirects[normalizedTitle] = wp.normalizeTitle(page.Redirect.Title)
		wp.redirectsMutex.Unlock()
		wp.updateStats(0, 1, 0)
		return
	}

	// Process article links
	links := wp.parseWikiTextForLinks(page.Revision.Text)

	if len(links) > 0 {
		// Resolve redirects for all links
		resolvedLinks := make([]string, 0, len(links))
		wp.redirectsMutex.RLock()
		for _, link := range links {
			if target, ok := wp.redirects[link]; ok {
				resolvedLinks = append(resolvedLinks, target)
			} else {
				resolvedLinks = append(resolvedLinks, link)
			}
		}
		wp.redirectsMutex.RUnlock()

		// Update adjacency list
		wp.adjListMutex.Lock()
		wp.adjList[normalizedTitle] = resolvedLinks
		wp.adjListMutex.Unlock()

		wp.updateStats(0, 0, 1)
	}
}

// updateStats safely updates processing statistics
func (wp *WikiProcessor) updateStats(pages, redirects, articles int64) {
	wp.statsMutex.Lock()
	wp.processedPages += pages
	wp.redirectCount += redirects
	wp.articleCount += articles
	wp.statsMutex.Unlock()
}

// getStats safely retrieves current statistics
func (wp *WikiProcessor) getStats() (int64, int64, int64) {
	wp.statsMutex.RLock()
	defer wp.statsMutex.RUnlock()
	return wp.processedPages, wp.redirectCount, wp.articleCount
}

// startWorkers launches worker goroutines
func (wp *WikiProcessor) startWorkers() {
	for i := 0; i < wp.numWorkers; i++ {
		go func() {
			for page := range wp.pageChannel {
				wp.processPage(page)
			}
		}()
	}
}

// ProcessDump processes the Wikipedia XML dump
func (wp *WikiProcessor) ProcessDump(xmlFilePath, outputPath string) error {
	log.Printf("Starting Wikipedia dump processing with %d workers...", wp.numWorkers)

	file, err := os.Open(xmlFilePath)
	if err != nil {
		return fmt.Errorf("error opening XML file: %w", err)
	}
	defer file.Close()

	var reader io.Reader = file
	if strings.HasSuffix(xmlFilePath, ".bz2") {
		log.Println("Decompressing bzip2 file...")
		reader = bzip2.NewReader(bufio.NewReader(file))
	} else {
		reader = bufio.NewReader(file)
	}

	decoder := xml.NewDecoder(reader)
	startTime := time.Now()

	// Start worker goroutines
	wp.startWorkers()

	// Progress reporting goroutine
	go wp.reportProgress(startTime)

	// Parse XML and send pages to workers
	for {
		tok, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("XML parsing error: %v", err)
			continue
		}

		if se, ok := tok.(xml.StartElement); ok && se.Name.Local == "page" {
			var page Page
			if err := decoder.DecodeElement(&page, &se); err != nil {
				log.Printf("Error decoding page: %v", err)
				continue
			}

			wp.wg.Add(1)
			wp.pageChannel <- page

			// Check if we've reached the maximum pages to process
			if wp.maxPages > 0 && wp.processedPages >= int64(wp.maxPages) {
				break
			}
		}
	}

	// Signal workers to stop and wait for completion
	close(wp.pageChannel)
	wp.wg.Wait()

	log.Printf("Processing completed in %s", time.Since(startTime))

	// Save the graph
	return wp.saveGraph(outputPath)
}

// reportProgress reports processing progress periodically
func (wp *WikiProcessor) reportProgress(startTime time.Time) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		pages, redirects, articles := wp.getStats()
		if pages == 0 {
			continue
		}

		elapsed := time.Since(startTime)
		rate := float64(pages) / elapsed.Seconds()

		log.Printf("Progress: %d pages processed (%.1f/sec), %d redirects, %d articles",
			pages, rate, redirects, articles)

		if wp.maxPages > 0 && pages >= int64(wp.maxPages) {
			break
		}
	}
}

// saveGraph saves the processed graph
func (wp *WikiProcessor) saveGraph(outputPath string) error {
	log.Printf("Saving graph with %d nodes...", len(wp.adjList))

	// Save as JSON (without indentation for smaller file size)
	jsonFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("error creating JSON file: %w", err)
	}
	defer jsonFile.Close()

	encoder := json.NewEncoder(jsonFile)
	// Remove indentation for faster loading
	// encoder.SetIndent("", "  ")

	if err := encoder.Encode(wp.adjList); err != nil {
		return fmt.Errorf("error encoding graph to JSON: %w", err)
	}

	// Also save as Gob format for faster loading
	gobPath := strings.TrimSuffix(outputPath, ".json") + ".gob"
	gobFile, err := os.Create(gobPath)
	if err != nil {
		log.Printf("Warning: couldn't create Gob file: %v", err)
	} else {
		defer gobFile.Close()
		gobEncoder := gob.NewEncoder(gobFile)
		if err := gobEncoder.Encode(wp.adjList); err != nil {
			log.Printf("Warning: error encoding graph to Gob: %v", err)
		} else {
			log.Println("Graph saved in Gob format for faster loading")
		}
	}

	log.Println("Graph saved successfully")
	return nil
}

func main() {
	// Configuration
    // Use project-local Data directory for input/exported dump files
    xmlFilePath := "Data/enwiki-latest-pages-articles.xml"
    outputPath := "Data/wikipedia_graph_go.json"
	maxPages := 0 // 0 for unlimited, set a number for testing

	processor := NewWikiProcessor(maxPages)

	if err := processor.ProcessDump(xmlFilePath, outputPath); err != nil {
		log.Fatalf("Error processing dump: %v", err)
	}

	pages, redirects, articles := processor.getStats()
	log.Printf("Final statistics: %d pages processed, %d redirects, %d articles with links",
		pages, redirects, articles)
}
