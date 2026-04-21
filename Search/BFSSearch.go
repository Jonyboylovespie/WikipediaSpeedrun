package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/edsrzf/mmap-go"
)

const graphFormatVersion = 1

// SerializedGraph is the compact on-disk format produced by Dump.
type SerializedGraph struct {
	Version uint32
	Titles  []string
	Edges   [][]uint32
}

// WikiGraph is an ID-based directed graph for fast shortest-path queries.
type WikiGraph struct {
	titles      []string
	edges       [][]uint32
	reverse     [][]uint32
	titleToID   map[string]uint32
	nodeCount   int
	totalEdges  int
	titleSearch []string
}

// PathResult contains the result of a shortest-path search.
type PathResult struct {
	Found        bool
	Path         []string
	PathLength   int
	SearchTime   time.Duration
	NodesVisited int
	StartOutDeg  int
	EndInDeg     int
}

func formatDurationMilliseconds(d time.Duration) string {
	if d <= 0 {
		return "<0.001ms"
	}
	return fmt.Sprintf("%.3fms", float64(d)/float64(time.Millisecond))
}

func loadSerializedGraph(path string) (*SerializedGraph, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error opening graph file: %w", err)
	}
	defer file.Close()

	data, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("error memory-mapping graph file: %w", err)
	}
	defer data.Unmap()

	var graph SerializedGraph
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&graph); err == nil {
		if graph.Version != graphFormatVersion {
			return nil, fmt.Errorf("unsupported graph version %d", graph.Version)
		}
		return &graph, nil
	}

	legacy, err := loadLegacyGraph(path)
	if err != nil {
		return nil, err
	}
	return legacy, nil
}

func loadLegacyGraph(path string) (*SerializedGraph, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error opening legacy graph file: %w", err)
	}
	defer file.Close()

	var adj map[string][]string
	if err := gob.NewDecoder(bufio.NewReader(file)).Decode(&adj); err != nil {
		return nil, fmt.Errorf("error decoding graph gob: %w", err)
	}

	log.Println("Loaded legacy graph format; converting to compact layout...")

	titles := make([]string, 0, len(adj))
	for title := range adj {
		titles = append(titles, title)
	}

	titleToID := make(map[string]uint32, len(titles))
	for i, title := range titles {
		titleToID[title] = uint32(i)
	}

	edges := make([][]uint32, len(titles))
	for i, title := range titles {
		neighbors := adj[title]
		if len(neighbors) == 0 {
			continue
		}

		mapped := make([]uint32, 0, len(neighbors))
		for _, neighbor := range neighbors {
			id, ok := titleToID[neighbor]
			if !ok {
				continue
			}
			mapped = append(mapped, id)
		}

		edges[i] = mapped
	}

	return &SerializedGraph{
		Version: graphFormatVersion,
		Titles:  titles,
		Edges:   edges,
	}, nil
}

func NewWikiGraphFromGob(path string) (*WikiGraph, error) {
	log.Printf("Loading Wikipedia graph from %s", path)
	start := time.Now()

	serialized, err := loadSerializedGraph(path)
	if err != nil {
		return nil, err
	}

	if len(serialized.Titles) != len(serialized.Edges) {
		return nil, fmt.Errorf("invalid graph: %d titles and %d edge rows", len(serialized.Titles), len(serialized.Edges))
	}

	titleToID := make(map[string]uint32, len(serialized.Titles))
	titleSearch := make([]string, len(serialized.Titles))
	for i, title := range serialized.Titles {
		titleToID[title] = uint32(i)
		titleSearch[i] = strings.ToLower(title)
	}

	reverse := make([][]uint32, len(serialized.Edges))
	totalEdges := 0
	for from, neighbors := range serialized.Edges {
		totalEdges += len(neighbors)
		for _, to := range neighbors {
			if int(to) >= len(reverse) {
				continue
			}
			reverse[to] = append(reverse[to], uint32(from))
		}
	}

	graph := &WikiGraph{
		titles:      serialized.Titles,
		edges:       serialized.Edges,
		reverse:     reverse,
		titleToID:   titleToID,
		nodeCount:   len(serialized.Titles),
		totalEdges:  totalEdges,
		titleSearch: titleSearch,
	}

	log.Printf("Graph loaded in %s. Nodes: %d, Edges: %d", time.Since(start), graph.nodeCount, graph.totalEdges)
	return graph, nil
}

func (wg *WikiGraph) normalizeTitle(title string) string {
	title = strings.TrimSpace(title)
	title = strings.ReplaceAll(title, "_", " ")
	if title == "" {
		return title
	}
	return strings.ToUpper(string(title[0])) + title[1:]
}

func (wg *WikiGraph) titleID(title string) (uint32, bool) {
	normalized := wg.normalizeTitle(title)
	id, ok := wg.titleToID[normalized]
	return id, ok
}

// ArticleExists checks if an article exists in the graph.
func (wg *WikiGraph) ArticleExists(title string) bool {
	_, ok := wg.titleID(title)
	return ok
}

// GetSimilarTitles finds articles with similar titles.
func (wg *WikiGraph) GetSimilarTitles(title string, maxResults int) []string {
	if maxResults <= 0 {
		return nil
	}

	normalized := strings.ToLower(wg.normalizeTitle(title))
	if normalized == "" {
		return nil
	}

	results := make([]string, 0, maxResults)
	for i, lower := range wg.titleSearch {
		if strings.Contains(lower, normalized) || strings.Contains(normalized, lower) {
			results = append(results, wg.titles[i])
			if len(results) >= maxResults {
				break
			}
		}
	}

	return results
}

func (wg *WikiGraph) expandFrontier(
	frontier []uint32,
	adj [][]uint32,
	visitedThis map[uint32]struct{},
	visitedOther map[uint32]struct{},
	parentThis map[uint32]uint32,
) ([]uint32, uint32, int, bool) {
	next := make([]uint32, 0, len(frontier)*4)
	newVisits := 0

	for _, node := range frontier {
		for _, neighbor := range adj[node] {
			if _, seen := visitedThis[neighbor]; seen {
				continue
			}

			visitedThis[neighbor] = struct{}{}
			parentThis[neighbor] = node
			newVisits++

			if _, found := visitedOther[neighbor]; found {
				return next, neighbor, newVisits, true
			}

			next = append(next, neighbor)
		}
	}

	return next, 0, newVisits, false
}

func reverseIDs(ids []uint32) {
	for left, right := 0, len(ids)-1; left < right; left, right = left+1, right-1 {
		ids[left], ids[right] = ids[right], ids[left]
	}
}

func (wg *WikiGraph) reconstructBidirectionalPath(
	meet, start, end uint32,
	parentStart map[uint32]uint32,
	parentEnd map[uint32]uint32,
) []string {
	left := make([]uint32, 0, 16)
	current := meet
	left = append(left, current)
	for current != start {
		current = parentStart[current]
		left = append(left, current)
	}
	reverseIDs(left)

	right := make([]uint32, 0, 16)
	current = meet
	for current != end {
		current = parentEnd[current]
		right = append(right, current)
	}

	pathIDs := append(left, right...)
	path := make([]string, len(pathIDs))
	for i, id := range pathIDs {
		path[i] = wg.titles[id]
	}

	return path
}

// BFS runs bidirectional BFS to find the shortest directed path.
func (wg *WikiGraph) BFS(startTitle, endTitle string) PathResult {
	searchStart := time.Now()
	result := PathResult{}

	startID, ok := wg.titleID(startTitle)
	if !ok {
		result.SearchTime = time.Since(searchStart)
		return result
	}

	endID, ok := wg.titleID(endTitle)
	if !ok {
		result.SearchTime = time.Since(searchStart)
		return result
	}

	result.StartOutDeg = len(wg.edges[startID])
	result.EndInDeg = len(wg.reverse[endID])

	if startID == endID {
		result.Found = true
		result.Path = []string{wg.titles[startID]}
		result.PathLength = 1
		result.NodesVisited = 1
		result.SearchTime = time.Since(searchStart)
		return result
	}

	frontStart := []uint32{startID}
	frontEnd := []uint32{endID}

	visitedStart := map[uint32]struct{}{startID: {}}
	visitedEnd := map[uint32]struct{}{endID: {}}
	parentStart := make(map[uint32]uint32)
	parentEnd := make(map[uint32]uint32)

	nodesVisited := 2

	for len(frontStart) > 0 && len(frontEnd) > 0 {
		if len(frontStart) <= len(frontEnd) {
			next, meet, added, found := wg.expandFrontier(frontStart, wg.edges, visitedStart, visitedEnd, parentStart)
			nodesVisited += added
			frontStart = next
			if found {
				path := wg.reconstructBidirectionalPath(meet, startID, endID, parentStart, parentEnd)
				result.Found = true
				result.Path = path
				result.PathLength = len(path)
				result.NodesVisited = nodesVisited
				result.SearchTime = time.Since(searchStart)
				return result
			}
		} else {
			next, meet, added, found := wg.expandFrontier(frontEnd, wg.reverse, visitedEnd, visitedStart, parentEnd)
			nodesVisited += added
			frontEnd = next
			if found {
				path := wg.reconstructBidirectionalPath(meet, startID, endID, parentStart, parentEnd)
				result.Found = true
				result.Path = path
				result.PathLength = len(path)
				result.NodesVisited = nodesVisited
				result.SearchTime = time.Since(searchStart)
				return result
			}
		}
	}

	result.NodesVisited = nodesVisited
	result.SearchTime = time.Since(searchStart)
	return result
}

// GetNodeStatistics returns node, edge, and average out-degree.
func (wg *WikiGraph) GetNodeStatistics() (int, int, float64) {
	avgDegree := 0.0
	if wg.nodeCount > 0 {
		avgDegree = float64(wg.totalEdges) / float64(wg.nodeCount)
	}
	return wg.nodeCount, wg.totalEdges, avgDegree
}

// DisplayPath prints search results.
func DisplayPath(result PathResult) {
	fmt.Println("\n" + strings.Repeat("=", 80))
	fmt.Println("WIKIPEDIA SHORTEST PATH RESULT")
	fmt.Println(strings.Repeat("=", 80))

	if !result.Found {
		fmt.Println("NO PATH FOUND")
		fmt.Printf("Search Time: %s\n", formatDurationMilliseconds(result.SearchTime))
		fmt.Printf("Nodes Visited: %d\n", result.NodesVisited)
		fmt.Printf("Start Out-Degree: %d\n", result.StartOutDeg)
		fmt.Printf("End In-Degree: %d\n", result.EndInDeg)
		fmt.Println(strings.Repeat("=", 80))
		return
	}

	fmt.Println("PATH FOUND")
	fmt.Printf("Path Length: %d degrees\n", result.PathLength-1)
	fmt.Printf("Search Time: %s\n", formatDurationMilliseconds(result.SearchTime))
	fmt.Printf("Nodes Visited: %d\n", result.NodesVisited)
	fmt.Println()

	for i, article := range result.Path {
		switch {
		case i == 0:
			fmt.Printf("START:      %s\n", article)
		case i == len(result.Path)-1:
			fmt.Printf("END:        %s\n", article)
		default:
			fmt.Printf("DEGREE %d:   %s\n", i, article)
		}
	}

	fmt.Println(strings.Repeat("=", 80))
}

func runInteractiveMode(graph *WikiGraph) {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("\nWIKIPEDIA PATHFINDER - Interactive Mode")
	fmt.Println("Type 'quit' to exit, 'stats' for graph statistics")

	for {
		fmt.Print("Enter start article: ")
		startArticle, _ := reader.ReadString('\n')
		startArticle = strings.TrimSpace(startArticle)

		switch strings.ToLower(startArticle) {
		case "quit":
			return
		case "stats":
			nodes, edges, avgDegree := graph.GetNodeStatistics()
			fmt.Printf("\nGraph Statistics:\n")
			fmt.Printf("  Nodes (Articles): %d\n", nodes)
			fmt.Printf("  Edges (Links): %d\n", edges)
			fmt.Printf("  Average Degree: %.2f\n\n", avgDegree)
			continue
		}

		fmt.Print("Enter end article: ")
		endArticle, _ := reader.ReadString('\n')
		endArticle = strings.TrimSpace(endArticle)

		if strings.ToLower(endArticle) == "quit" {
			return
		}

		if !graph.ArticleExists(startArticle) {
			fmt.Printf("Start article '%s' not found.\n", startArticle)
			similar := graph.GetSimilarTitles(startArticle, 5)
			if len(similar) > 0 {
				fmt.Println("Did you mean:")
				for _, title := range similar {
					fmt.Printf("  - %s\n", title)
				}
			}
			fmt.Println()
			continue
		}

		if !graph.ArticleExists(endArticle) {
			fmt.Printf("End article '%s' not found.\n", endArticle)
			similar := graph.GetSimilarTitles(endArticle, 5)
			if len(similar) > 0 {
				fmt.Println("Did you mean:")
				for _, title := range similar {
					fmt.Printf("  - %s\n", title)
				}
			}
			fmt.Println()
			continue
		}

		result := graph.BFS(startArticle, endArticle)
		DisplayPath(result)
		fmt.Println()
	}
}

func main() {
	graphFilePath := flag.String("graph", "C:/WikiDump/wikipedia_graph_go.gob", "Path to graph Gob file")
	startArticle := flag.String("start", "", "Start article for one-shot search")
	endArticle := flag.String("end", "", "End article for one-shot search")
	flag.Parse()

	graph, err := NewWikiGraphFromGob(*graphFilePath)
	if err != nil {
		log.Fatalf("Failed to load graph: %v", err)
	}

	nodes, edges, avgDegree := graph.GetNodeStatistics()
	fmt.Printf("Loaded Wikipedia Graph:\n")
	fmt.Printf("  Articles: %d\n", nodes)
	fmt.Printf("  Links: %d\n", edges)
	fmt.Printf("  Average links per article: %.2f\n\n", avgDegree)

	if *startArticle != "" && *endArticle != "" {
		result := graph.BFS(*startArticle, *endArticle)
		DisplayPath(result)
		return
	}

	runInteractiveMode(graph)
	fmt.Println("Thanks for using Wikipedia Pathfinder!")
}
