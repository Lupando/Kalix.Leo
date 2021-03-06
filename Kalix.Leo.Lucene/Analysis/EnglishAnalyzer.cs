﻿using Lucene.Net.Analysis;
using Lucene.Net.Analysis.Core;
using Lucene.Net.Analysis.En;
using Lucene.Net.Analysis.Miscellaneous;
using Lucene.Net.Analysis.Util;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Kalix.Leo.Lucene.Analysis
{
    /// <summary>
    /// Analyser with a number of common stop words and with appropriate lucene filters
    /// </summary>
    public class EnglishAnalyzer : Analyzer
    {
        private static readonly string[] _stopWords = new[]
        {
            "0", "1", "2", "3", "4", "5", "6", "7", "8",
            "9", "000", "$", "£",
            "about", "after", "all", "also", "an", "and",
            "another", "any", "are", "as", "at", "be",
            "because", "been", "before", "being", "between",
            "both", "but", "by", "came", "can", "come",
            "could", "did", "do", "does", "each", "else",
            "for", "from", "get", "got", "has", "had",
            "he", "have", "her", "here", "him", "himself",
            "his", "how","if", "in", "into", "is", "it",
            "its", "just", "like", "make", "many", "me",
            "might", "more", "most", "much", "must", "my",
            "never", "now", "of", "on", "only", "or",
            "other", "our", "out", "over", "re", "said",
            "same", "see", "should", "since", "so", "some",
            "still", "such", "take", "than", "that", "the",
            "their", "them", "then", "there", "these",
            "they", "this", "those", "through", "to", "too",
            "under", "up", "use", "very", "want", "was",
            "way", "we", "well", "were", "what", "when",
            "where", "which", "while", "who", "will",
            "with", "would", "you", "your",
            "a", "b", "c", "d", "e", "f", "g", "h", "i",
            "j", "k", "l", "m", "n", "o", "p", "q", "r",
            "s", "t", "u", "v", "w", "x", "y", "z"
        };

        private readonly CharArraySet _words;

        /// <summary>
        /// Constructor with default stop words
        /// </summary>
        public EnglishAnalyzer()
            : this(_stopWords)
        {

        }

        /// <summary>
        /// Constructor that allows you to specify your stop words
        /// </summary>
        /// <param name="stopWords">Stopwords to use (lucene will not index these words) - should be all lowercase</param>
        public EnglishAnalyzer(IEnumerable<string> stopWords)
        {
            _words = StopFilter.MakeStopSet(LeoLuceneVersion.Version, stopWords.ToArray());
        }
        
        /// <summary>
        /// Override of the token stream method, uses these filters in order:
        /// 
        /// Whitespace splitter
        /// ASCII common folder (ie é goes to e)
        /// Lowercase
        /// Stopwords removed
        /// Porter stemming (reduces words to common stem)
        /// </summary>
        protected override TokenStreamComponents CreateComponents(string fieldName, TextReader reader)
        {
            var tokenizer = new WhitespaceTokenizer(LeoLuceneVersion.Version, reader);
            TokenStream filter = new ASCIIFoldingFilter(tokenizer);
            filter = new LowerCaseFilter(LeoLuceneVersion.Version, filter);
            filter = new StopFilter(LeoLuceneVersion.Version, filter, _words);
            filter = new PorterStemFilter(filter);
            return new TokenStreamComponents(tokenizer, filter);
        }
    }
}
