package main

import (
	"bufio"
	"compress/bzip2"
	"encoding/gob"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const graphFormatVersion = 1

var wikiLinkPattern = regexp.MustCompile(`\[\[([^|\[\]#]+)(?:#[^\[\]]*)?(?:\|[^\[\]]*)?\]\]`)

var ignoredNamespaces = map[string]struct{}{
	"Category":  {},
	"Template":  {},
	"File":      {},
	"Image":     {},
	"Media":     {},
	"Special":   {},
	"User":      {},
	"Talk":      {},
	"Wikipedia": {},
	"Help":      {},
}

// Page represents a Wikipedia page structure from the XML.
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

// SerializedGraph is the compact on-disk format for search.
type SerializedGraph struct {
	Version uint32
	Titles  []string
	Edges   [][]uint32
}

// WikiProcessor handles the processing of Wikipedia dumps.
type WikiProcessor struct {
	redirects      map[string]string
	adjList        map[string][]string
	redirectsMutex sync.RWMutex
	adjListMutex   sync.RWMutex

	processedPages int64
	redirectCount  int64
	articleCount   int64

	maxPages    int
	numWorkers  int
	writeJSON   bool
	pageChannel chan Page
	wg          sync.WaitGroup
}

// NewWikiProcessor creates a new Wikipedia processor.
func NewWikiProcessor(maxPages, numWorkers int, writeJSON bool) *WikiProcessor {
	if numWorkers < 1 {
		numWorkers = 1
	}

	return &WikiProcessor{
		redirects:   make(map[string]string),
		adjList:     make(map[string][]string),
		maxPages:    maxPages,
		numWorkers:  numWorkers,
		writeJSON:   writeJSON,
		pageChannel: make(chan Page, numWorkers*2),
	}
}

// normalizeTitle normalizes article titles for consistent lookup.
func (wp *WikiProcessor) normalizeTitle(title string) string {
	title = strings.TrimSpace(title)
	title = strings.ReplaceAll(title, "_", " ")

	if len(title) == 0 {
		return title
	}

	return strings.ToUpper(string(title[0])) + title[1:]
}

// parseWikiTextForLinks extracts wiki links efficiently.
func (wp *WikiProcessor) parseWikiTextForLinks(text string) []string {
	matches := wikiLinkPattern.FindAllStringSubmatch(text, -1)
	if len(matches) == 0 {
		return nil
	}

	links := make([]string, 0, len(matches))
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}

		link := wp.normalizeTitle(match[1])
		if link == "" || wp.isIgnoredNamespace(link) {
			continue
		}

		links = append(links, link)
	}

	return wp.deduplicateLinks(links)
}

// isIgnoredNamespace checks whether a link should be excluded.
func (wp *WikiProcessor) isIgnoredNamespace(title string) bool {
	colon := strings.IndexByte(title, ':')
	if colon <= 0 {
		return false
	}

	_, ignored := ignoredNamespaces[title[:colon]]
	return ignored
}

// deduplicateLinks removes duplicate links while preserving order.
func (wp *WikiProcessor) deduplicateLinks(links []string) []string {
	seen := make(map[string]struct{}, len(links))
	result := make([]string, 0, len(links))

	for _, link := range links {
		if _, ok := seen[link]; ok {
			continue
		}
		seen[link] = struct{}{}
		result = append(result, link)
	}

	return result
}

// processPage handles individual page processing.
func (wp *WikiProcessor) processPage(page Page) {
	defer wp.wg.Done()

	wp.updateStats(1, 0, 0)

	normalizedTitle := wp.normalizeTitle(page.Title)
	if page.NS != 0 || normalizedTitle == "" {
		return
	}

	if page.Redirect.Title != "" {
		wp.redirectsMutex.Lock()
		wp.redirects[normalizedTitle] = wp.normalizeTitle(page.Redirect.Title)
		wp.redirectsMutex.Unlock()
		wp.updateStats(0, 1, 0)
		return
	}

	links := wp.parseWikiTextForLinks(page.Revision.Text)

	wp.adjListMutex.Lock()
	wp.adjList[normalizedTitle] = links
	wp.adjListMutex.Unlock()

	wp.updateStats(0, 0, 1)
}

func (wp *WikiProcessor) updateStats(pages, redirects, articles int64) {
	if pages != 0 {
		atomic.AddInt64(&wp.processedPages, pages)
	}
	if redirects != 0 {
		atomic.AddInt64(&wp.redirectCount, redirects)
	}
	if articles != 0 {
		atomic.AddInt64(&wp.articleCount, articles)
	}
}

func (wp *WikiProcessor) getStats() (int64, int64, int64) {
	return atomic.LoadInt64(&wp.processedPages),
		atomic.LoadInt64(&wp.redirectCount),
		atomic.LoadInt64(&wp.articleCount)
}

// startWorkers launches worker goroutines.
func (wp *WikiProcessor) startWorkers() {
	for i := 0; i < wp.numWorkers; i++ {
		go func() {
			for page := range wp.pageChannel {
				wp.processPage(page)
			}
		}()
	}
}

// ProcessDump processes the Wikipedia XML dump.
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
		reader = bzip2.NewReader(bufio.NewReaderSize(file, 4<<20))
	} else {
		reader = bufio.NewReaderSize(file, 4<<20)
	}

	decoder := xml.NewDecoder(reader)
	startTime := time.Now()

	wp.startWorkers()
	progressDone := make(chan struct{})
	go wp.reportProgress(startTime, progressDone)

	pagesQueued := 0
	for {
		tok, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("XML parsing error: %v", err)
			continue
		}

		se, ok := tok.(xml.StartElement)
		if !ok || se.Name.Local != "page" {
			continue
		}

		if wp.maxPages > 0 && pagesQueued >= wp.maxPages {
			break
		}

		var page Page
		if err := decoder.DecodeElement(&page, &se); err != nil {
			log.Printf("Error decoding page: %v", err)
			continue
		}

		wp.wg.Add(1)
		wp.pageChannel <- page
		pagesQueued++
	}

	close(wp.pageChannel)
	wp.wg.Wait()
	close(progressDone)

	log.Printf("Processing completed in %s", time.Since(startTime))
	return wp.saveGraph(outputPath)
}

// reportProgress reports processing progress periodically.
func (wp *WikiProcessor) reportProgress(startTime time.Time, done <-chan struct{}) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			pages, redirects, articles := wp.getStats()
			if pages == 0 {
				continue
			}

			elapsed := time.Since(startTime)
			rate := float64(pages) / elapsed.Seconds()

			log.Printf("Progress: %d pages processed (%.1f/sec), %d redirects, %d articles",
				pages, rate, redirects, articles)
		}
	}
}

func resolveRedirectTarget(title string, redirects map[string]string, titleToID map[string]uint32, cache map[string]string) (string, bool) {
	if _, ok := titleToID[title]; ok {
		return title, true
	}

	if cached, ok := cache[title]; ok {
		if cached == "" {
			return "", false
		}
		return cached, true
	}

	current := title
	seen := make(map[string]struct{}, 4)
	for {
		next, ok := redirects[current]
		if !ok {
			break
		}

		if _, cycle := seen[current]; cycle {
			cache[title] = ""
			return "", false
		}
		seen[current] = struct{}{}
		current = next
	}

	if _, ok := titleToID[current]; !ok {
		cache[title] = ""
		return "", false
	}

	cache[title] = current
	return current, true
}

func (wp *WikiProcessor) buildSerializedGraph() (*SerializedGraph, error) {
	titles := make([]string, 0, len(wp.adjList))
	for title := range wp.adjList {
		titles = append(titles, title)
	}
	sort.Strings(titles)

	titleToID := make(map[string]uint32, len(titles))
	for i, title := range titles {
		titleToID[title] = uint32(i)
	}

	redirects := make(map[string]string, len(wp.redirects))
	for from, to := range wp.redirects {
		redirects[from] = to
	}

	edges := make([][]uint32, len(titles))
	resolvedCache := make(map[string]string, len(redirects))

	for sourceID, sourceTitle := range titles {
		rawLinks := wp.adjList[sourceTitle]
		if len(rawLinks) == 0 {
			continue
		}

		seen := make(map[uint32]struct{}, len(rawLinks))
		resolvedLinks := make([]uint32, 0, len(rawLinks))

		for _, link := range rawLinks {
			resolved, ok := resolveRedirectTarget(link, redirects, titleToID, resolvedCache)
			if !ok {
				continue
			}

			id := titleToID[resolved]
			if _, dup := seen[id]; dup {
				continue
			}

			seen[id] = struct{}{}
			resolvedLinks = append(resolvedLinks, id)
		}

		edges[sourceID] = resolvedLinks
	}

	return &SerializedGraph{
		Version: graphFormatVersion,
		Titles:  titles,
		Edges:   edges,
	}, nil
}

// saveGraph saves the processed graph in compact Gob form.
func (wp *WikiProcessor) saveGraph(outputPath string) error {
	log.Printf("Building compact graph from %d articles...", len(wp.adjList))

	graph, err := wp.buildSerializedGraph()
	if err != nil {
		return fmt.Errorf("error building compact graph: %w", err)
	}

	log.Printf("Saving Gob graph to %s", outputPath)
	gobFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("error creating Gob file: %w", err)
	}
	defer gobFile.Close()

	buffered := bufio.NewWriterSize(gobFile, 4<<20)
	gobEncoder := gob.NewEncoder(buffered)
	if err := gobEncoder.Encode(graph); err != nil {
		return fmt.Errorf("error encoding graph to Gob: %w", err)
	}
	if err := buffered.Flush(); err != nil {
		return fmt.Errorf("error flushing Gob file: %w", err)
	}

	if wp.writeJSON {
		jsonPath := strings.TrimSuffix(outputPath, filepath.Ext(outputPath)) + ".json"
		if err := saveGraphJSON(jsonPath, graph); err != nil {
			return err
		}
	}

	log.Println("Graph saved successfully")
	return nil
}

func saveGraphJSON(outputPath string, graph *SerializedGraph) error {
	log.Printf("Saving JSON graph to %s", outputPath)

	jsonFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("error creating JSON file: %w", err)
	}
	defer jsonFile.Close()

	buffered := bufio.NewWriterSize(jsonFile, 4<<20)
	encoder := json.NewEncoder(buffered)
	if err := encoder.Encode(graph); err != nil {
		return fmt.Errorf("error encoding graph to JSON: %w", err)
	}

	if err := buffered.Flush(); err != nil {
		return fmt.Errorf("error flushing JSON file: %w", err)
	}

	return nil
}

func main() {
	xmlFilePath := flag.String("input", "C:/WikiDump/enwiki-latest-pages-articles.xml", "Path to Wikipedia XML dump (.xml or .bz2)")
	outputPath := flag.String("output", "C:/WikiDump/wikipedia_graph_go.gob", "Path for output Gob graph")
	maxPages := flag.Int("max-pages", 0, "Maximum pages to process (0 = unlimited)")
	workers := flag.Int("workers", runtime.NumCPU(), "Number of worker goroutines")
	writeJSON := flag.Bool("write-json", false, "Also write a JSON graph file")
	flag.Parse()

	processor := NewWikiProcessor(*maxPages, *workers, *writeJSON)

	if err := processor.ProcessDump(*xmlFilePath, *outputPath); err != nil {
		log.Fatalf("Error processing dump: %v", err)
	}

	pages, redirects, articles := processor.getStats()
	log.Printf("Final statistics: %d pages processed, %d redirects, %d articles",
		pages, redirects, articles)
}
