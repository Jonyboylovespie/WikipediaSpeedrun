package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type ProgressReader struct {
	Reader     io.Reader
	Size       int64
	BytesRead  int64
	ReportFunc func(bytesRead, total int64)
}

func (pr *ProgressReader) Read(p []byte) (int, error) {
	n, err := pr.Reader.Read(p)
	pr.BytesRead += int64(n)
	if pr.ReportFunc != nil {
		pr.ReportFunc(pr.BytesRead, pr.Size)
	}
	return n, err
}

// WikiGraph represents the Wikipedia link graph
type WikiGraph struct {
	adjacencyList map[string][]string
	nodeCount     int
}

// PathResult contains the result of a BFS search
type PathResult struct {
	Found        bool
	Path         []string
	PathLength   int
	SearchTime   time.Duration
	NodesVisited int
}

// Helper to create a progress bar display
func createProgressBar(filePath string) (*os.File, *ProgressReader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, err
	}

	reader := &ProgressReader{
		Reader: file,
		Size:   info.Size(),
		ReportFunc: func(bytesRead, total int64) {
			percent := float64(bytesRead) * 100 / float64(total)
			width := 40
			completed := int(percent * float64(width) / 100)

			fmt.Printf("\r[%s%s] %.1f%% ",
				strings.Repeat("=", completed),
				strings.Repeat(" ", width-completed),
				percent)
		},
	}

	return file, reader, nil
}

func NewWikiGraphFromGob(filepath string) (*WikiGraph, error) {
	log.Printf("Loading Wikipedia graph from Gob: %s", filepath)
	startTime := time.Now()

	// Open the file
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	defer file.Close()

	// Memory map the file
	mmapData, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("error memory mapping file: %w", err)
	}
	defer mmapData.Unmap()

	// Decode directly from the memory-mapped data
	fmt.Println("Decoding graph data from memory-mapped file...")
	decodeStart := time.Now()

	var adjList map[string][]string
	decoder := gob.NewDecoder(bytes.NewReader(mmapData))
	if err := decoder.Decode(&adjList); err != nil {
		return nil, fmt.Errorf("error decoding Gob: %w", err)
	}

	loadTime := time.Since(startTime)
	decodeTime := time.Since(decodeStart)

	log.Printf("Graph loaded in %s (mmap: %s, decode: %s). Nodes: %d",
		loadTime, loadTime-decodeTime, decodeTime, len(adjList))

	return &WikiGraph{
		adjacencyList: adjList,
		nodeCount:     len(adjList),
	}, nil
}

// normalizeTitle normalizes article titles for consistent lookup
func (wg *WikiGraph) normalizeTitle(title string) string {
	title = strings.TrimSpace(title)
	title = strings.ReplaceAll(title, "_", " ")

	if len(title) == 0 {
		return title
	}

	// Basic capitalization
	return strings.ToUpper(string(title[0])) + title[1:]
}

// ArticleExists checks if an article exists in the graph
func (wg *WikiGraph) ArticleExists(title string) bool {
	normalized := wg.normalizeTitle(title)
	_, exists := wg.adjacencyList[normalized]
	return exists
}

// GetSimilarTitles finds articles with similar titles (for suggestions)
func (wg *WikiGraph) GetSimilarTitles(title string, maxResults int) []string {
	normalized := strings.ToLower(wg.normalizeTitle(title))
	var similar []string

	count := 0
	for articleTitle := range wg.adjacencyList {
		if count >= maxResults {
			break
		}

		lowerTitle := strings.ToLower(articleTitle)
		if strings.Contains(lowerTitle, normalized) || strings.Contains(normalized, lowerTitle) {
			similar = append(similar, articleTitle)
			count++
		}
	}

	return similar
}

// BFS performs breadth-first search to find shortest path
func (wg *WikiGraph) BFS(startTitle, endTitle string) PathResult {
	start := wg.normalizeTitle(startTitle)
	end := wg.normalizeTitle(endTitle)

	searchStart := time.Now()

	result := PathResult{
		Found:        false,
		Path:         nil,
		PathLength:   0,
		NodesVisited: 0,
	}

	// Check if start and end articles exist
	if !wg.ArticleExists(start) {
		log.Printf("Start article '%s' not found in graph", start)
		result.SearchTime = time.Since(searchStart)
		return result
	}

	if !wg.ArticleExists(end) {
		log.Printf("End article '%s' not found in graph", end)
		result.SearchTime = time.Since(searchStart)
		return result
	}

	// Handle case where start equals end
	if start == end {
		result.Found = true
		result.Path = []string{start}
		result.PathLength = 1
		result.NodesVisited = 1
		result.SearchTime = time.Since(searchStart)
		return result
	}

	// BFS implementation
	queue := []string{start}
	visited := make(map[string]bool)
	parent := make(map[string]string)

	visited[start] = true
	nodesVisited := 1

	log.Printf("Starting BFS from '%s' to '%s'...", start, end)

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		// Check neighbors
		for _, neighbor := range wg.adjacencyList[current] {
			if !visited[neighbor] {
				visited[neighbor] = true
				parent[neighbor] = current
				queue = append(queue, neighbor)
				nodesVisited++

				// Found the target
				if neighbor == end {
					// Reconstruct path
					path := wg.reconstructPath(parent, start, end)

					result.Found = true
					result.Path = path
					result.PathLength = len(path)
					result.NodesVisited = nodesVisited
					result.SearchTime = time.Since(searchStart)

					return result
				}
			}
		}

		// Progress reporting for long searches
		if nodesVisited%1000000 == 0 {
			elapsed := time.Since(searchStart)
			log.Printf("BFS progress: %d nodes visited, %d in queue, elapsed: %s",
				nodesVisited, len(queue), elapsed)
		}

		// Safety check to prevent infinite search
		if nodesVisited > 1000000000 { // 1 billion nodes
			log.Printf("Search stopped after visiting %d nodes (safety limit)", nodesVisited)
			break
		}
	}

	result.NodesVisited = nodesVisited
	result.SearchTime = time.Since(searchStart)
	log.Printf("No path found. Nodes visited: %d, Time: %s", nodesVisited, result.SearchTime)

	return result
}

// reconstructPath rebuilds the path from parent pointers
func (wg *WikiGraph) reconstructPath(parent map[string]string, start, end string) []string {
	path := []string{}
	current := end

	for current != start {
		path = append([]string{current}, path...)
		current = parent[current]
	}

	path = append([]string{start}, path...)
	return path
}

// GetNodeStatistics returns basic statistics about the graph
func (wg *WikiGraph) GetNodeStatistics() (int, int, float64) {
	totalEdges := 0
	for _, links := range wg.adjacencyList {
		totalEdges += len(links)
	}

	avgDegree := float64(totalEdges) / float64(wg.nodeCount)
	return wg.nodeCount, totalEdges, avgDegree
}

// DisplayPath formats and displays the path nicely
func DisplayPath(result PathResult) {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("WIKIPEDIA SHORTEST PATH RESULT")
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println()

	if !result.Found {
		fmt.Println("❌ NO PATH FOUND")
		fmt.Printf("⏱️ Search Time: %s\n", result.SearchTime)
		fmt.Printf("🔍 Nodes Visited: %d\n", result.NodesVisited)
	} else {
		fmt.Println("✅ PATH FOUND!")
		fmt.Printf("📏 Path Length: %d degrees\n", result.PathLength-1) // Exclude start article from length
		fmt.Printf("⏱️ Search Time: %s\n", result.SearchTime)
		fmt.Printf("🔍 Nodes Visited: %d\n", result.NodesVisited)
		fmt.Println()

		for i, article := range result.Path {
			if i == 0 {
				fmt.Printf("🚀 START:     %s\n", article)
			} else if i == len(result.Path)-1 {
				fmt.Printf("🎯 END:       %s\n", article)
			} else {
				fmt.Printf("⬇️ DEGREE %d:  %s\n", i, article)
			}
		}
	}

	fmt.Println()
	fmt.Println(strings.Repeat("=", 80))
}

// Interactive mode for user input
func runInteractiveMode(graph *WikiGraph) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("\n🌟 WIKIPEDIA PATHFINDER - Interactive Mode")
	fmt.Println("Type 'quit' to exit, 'stats' for graph statistics")
	fmt.Println()

	for {
		fmt.Print("Enter start article: ")
		startArticle, _ := reader.ReadString('\n')
		startArticle = strings.TrimSpace(startArticle)

		if strings.ToLower(startArticle) == "quit" {
			break
		}

		if strings.ToLower(startArticle) == "stats" {
			nodes, edges, avgDegree := graph.GetNodeStatistics()
			fmt.Printf("\n📊 Graph Statistics:\n")
			fmt.Printf("   Nodes (Articles): %d\n", nodes)
			fmt.Printf("   Edges (Links): %d\n", edges)
			fmt.Printf("   Average Degree: %.2f\n\n", avgDegree)
			continue
		}

		fmt.Print("Enter end article: ")
		endArticle, _ := reader.ReadString('\n')
		endArticle = strings.TrimSpace(endArticle)

		if strings.ToLower(endArticle) == "quit" {
			break
		}

		// Check if articles exist and provide suggestions
		if !graph.ArticleExists(startArticle) {
			fmt.Printf("❌ Start article '%s' not found.\n", startArticle)
			similar := graph.GetSimilarTitles(startArticle, 5)
			if len(similar) > 0 {
				fmt.Println("💡 Did you mean one of these?")
				for _, title := range similar {
					fmt.Printf("   - %s\n", title)
				}
			}
			fmt.Println()
			continue
		}

		if !graph.ArticleExists(endArticle) {
			fmt.Printf("❌ End article '%s' not found.\n", endArticle)
			similar := graph.GetSimilarTitles(endArticle, 5)
			if len(similar) > 0 {
				fmt.Println("💡 Did you mean one of these?")
				for _, title := range similar {
					fmt.Printf("   - %s\n", title)
				}
			}
			fmt.Println()
			continue
		}

		// Perform BFS
		result := graph.BFS(startArticle, endArticle)
		DisplayPath(result)
		fmt.Println()
	}
}

func main() {
	cur, err := os.Getwd()
	if err != nil {
		log.Fatalf("Error: %v", err)
		return
	}
	dir := filepath.Dir(cur)
	
    graphFilePath := filepath.Join(dir, "Data", "wikipedia_graph_go.json")

	// Check if custom path is provided as command line argument
	if len(os.Args) > 1 {
		graphFilePath = os.Args[1]
	}

	// Load the Wikipedia graph
	graph, err := NewWikiGraphFromGob(graphFilePath)
	if err != nil {
		log.Fatalf("Failed to load graph: %v", err)
	}

	nodes, edges, avgDegree := graph.GetNodeStatistics()
	fmt.Printf("📊 Loaded Wikipedia Graph:\n")
	fmt.Printf("   Articles: %d\n", nodes)
	fmt.Printf("   Links: %d\n", edges)
	fmt.Printf("   Average links per article: %.2f\n\n", avgDegree)

	// Start interactive mode
	runInteractiveMode(graph)

	fmt.Println("👋 Thanks for using Wikipedia Pathfinder!")
}
