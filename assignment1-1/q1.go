//Authors: Roberto Julio Saldana Hernandez A00354886
//		  Juan Antonio Cuellar de la pena A00354849
//   	  Ricardo Arturo Benitez Cruz A01018084
//        Jose Roberto Calderon Samado A00354818


package cos418_hw1_1
//package main

import (
	"fmt"
	"sort"
	"io/ioutil"
	"log"
	"strings"
	"regexp"
)

func main(){
	topWords("simple.txt",5,2)
}

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

	//Return variable
	var result []WordCount

	//Reading the file 
	content, err := ioutil.ReadFile(path)

	if err != nil {
		log.Fatal(err)
		return nil
	}

	//Removing all punctuations
	scontent := stripNonAlphanumeric(strings.ToLower(string(content)))

	//Using Fields to iterate between words
	//I am also using ToLower function to convert all content to lowercase
	words := strings.Fields(scontent)

	//counting words
	for _, y := range words {
		
		if(len(y) >= charThreshold && !wordCounted(result,y)){
			newWord := WordCount{y,1}
			result = append(result,newWord)
		}
	}

	sortWordCounts(result)

	return result[0:numWords]
}

func wordCounted(words []WordCount, word string) bool {
	for x, y := range words {
		if(y.Word == word){
			words[x].Count++
			return true
		}
	}

	return false
}

func stripNonAlphanumeric(in string) string{
	//I had to modify a little the regex to exclude space as non-alphanumeric strip
	reg, _ := regexp.Compile("[^0-9a-zA-Z\\s]+")
	return reg.ReplaceAllString(in,"")
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
