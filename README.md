# News Deframer - Trend Mining Engine

This is the Trend Mining component for the main [News Deframer](https://github.com/deframer/news-deframer) project.

## Why Trend Mining?

This component shows current talking points and visualizes rising trends over time. It also allows comparing coverage across different news sources and countries.

As a bonus, it helps to uncover "blind spots." Relying on a small set of news feeds can create gaps regarding important events. By showing what others are reading, this helps to discover relevant content outside of the usual bubble.

This approach may be based on the findings in this [PhD Thesis](https://refubium.fu-berlin.de/bitstream/handle/fub188/7212/streibel-diss-online-1.pdf?sequence=1&isAllowed=y).


## Why Sentiments?

We use a VAD/VAC (Dimensional) and BE5 (Discrete) approach to detect sentiments and emotions in texts, leveraging sentiment scores from [MEmoLon](https://github.com/JULIELab/MEmoLon), an emotion lexicon for 90+ languages. The **VAD** (Valence, Arousal, Dominance) model evaluates overall mood on a 1-9 scale across Valence (polarity/pleasantness), Arousal (activation/excitement), and Dominance (perceived control). The **BE5** model measures the intensity of discrete emotions—Joy, Anger, Sadness, Fear, and Disgust—on a 1-5 scale.

Models are lazily loaded: on first use they download into ignored `models/spacy/` and `models/memolon/` directories, then stay cached in memory for the running process.

There is fundamental science supporting this methodology: fMRT experiments demonstrate that reading specific words can indeed induce measurable emotional responses in the brain. For more details on this theory, refer to this [PhD thesis](https://edoc.ub.uni-muenchen.de/18933/1/Danner_Sandro_C.pdf).


## License

[MIT](LICENSE.md)
