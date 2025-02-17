package cos418_hw1_1

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"
)

// Find the top K most common words in a text document.
// 	path: location of the document
//	numWords: number of words to return (i.e. k)
//	charThreshold: character threshold for whether a token qualifies as a word,
//		e.g. charThreshold = 5 means "apple" is a word but "pear" is not.
// Matching is case insensitive, e.g. "Orange" and "orange" is considered the same word.
// A word comprises alphanumeric characters only. All punctuations and other characters
// are removed, e.g. "don't" becomes "dont".
// You should use `checkError` to handle potential errors.
func topWords(path string, numWords int, charThreshold int) []WordCount {
	// TODO: implement me
	// HINT: You may find the `strings.Fields` and `strings.ToLower` functions helpful
	// HINT: To keep only alphanumeric characters, use the regex "[^0-9a-zA-Z]+"

	wordMap := make(map[string]int)
	regex, err := regexp.Compile("[^0-9a-zA-Z]+")
	checkError(err)

	readFile(path, func(lineText string) {
		words := strings.Fields(lineText)
		for _, word := range words {

			if len(word) == 0 || len(word) < charThreshold {
				continue
			}

			word = strings.ToLower(word)

			if regex.Match([]byte(word)) {
				var newWord string
				for i := 0; i < len(word); i++ {
					c := word[i]
					if c >= '0' && c <= '9' || c >= 'a' && c <= 'z' {
						newWord += fmt.Sprintf("%c", c)
					}
				}
				word = newWord
			}

			wordMap[word] += 1
		}
	})

	var wordCounts []WordCount
	for key, value := range wordMap {
		wordCounts = append(wordCounts, WordCount{
			Word:  key,
			Count: value,
		})
	}
	sortWordCounts(wordCounts)
	return wordCounts[0:numWords]
}

func readFile(path string, lineReadCallback func(lineText string)) {
	// open file
	f, err := os.Open(path)
	checkError(err)
	// remember to close the file at the end of the program
	defer f.Close()

	// read the file line by line using scanner
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		lineReadCallback(scanner.Text())
	}

	checkError(scanner.Err())
}

// A struct that represents how many times a word is observed in a document
type WordCount struct {
	Word  string
	Count int
}

func (wc WordCount) String() string {
	return fmt.Sprintf("%v: %v", wc.Word, wc.Count)
}

// Helper function to sort a list of word counts in place.
// This sorts by the count in decreasing order, breaking ties using the word.
// DO NOT MODIFY THIS FUNCTION!
func sortWordCounts(wordCounts []WordCount) {
	sort.Slice(wordCounts, func(i, j int) bool {
		wc1 := wordCounts[i]
		wc2 := wordCounts[j]
		if wc1.Count == wc2.Count {
			return wc1.Word < wc2.Word
		}
		return wc1.Count > wc2.Count
	})
}
