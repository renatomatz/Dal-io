import os
import importlib
import collections

import nltk
import pandas as pd

from dalio.pipe.extra_classes import _ColValSelection

from dalio.util import out_of_place_col_insert

from dalio.pipe import MapColVals


class TokenizeWords(MapColVals):
    """A pipeline stage that tokenize a sentence into words by whitespaces.

    Note: The nltk package must be installed for this pipeline stage to work.

    Parameters
    ----------
    columns : str or list-like
        Column names in the DataFrame to be tokenized.
    drop : bool, default True
        If set to True, the source columns are dropped after being tokenized,
        and the resulting tokenized columns retain the names of the source
        columns. Otherwise, tokenized columns gain the suffix '_tok'.

    Example
    -------
        >>> import pandas as pd; import pdpipe as pdp;
        >>> df = pd.DataFrame(
        ...     [[3.2, "Kick the baby!"]], [1], ['freq', 'content'])
        >>> tokenize_stage = pdp.TokenizeWords('content')
        >>> tokenize_stage(df)
           freq               content
        1   3.2  [Kick, the, baby, !]
    """

    def __init__(self, columns, drop=True, **kwargs):
        super().__init__(
            columns=columns,
            value_map=nltk.word_tokenize,
            drop=drop
        )

        # TODO add Description to make sure all columns are of "object" dtype

        # ensure nltk punkt is downloaded
        try:
            nltk.word_tokenize('a a')
        except LookupError:  # pragma: no cover
            # try:
            #     nltk.data.find('corpora/stopwords')
            # except LookupError:  # pragma: no cover
            dpath = os.path.expanduser('~/nltk_data/tokenizers')
            os.makedirs(dpath, exist_ok=True)
            nltk.download('punkt')


class UntokenizeWords(MapColVals):
    """A pipeline stage that joins token lists to whitespace-seperated strings.

    Note: The nltk package must be installed for this pipeline stage to work.

    Parameters
    ----------
    columns : str or list-like
        Column names in the DataFrame to be untokenized.
    drop : bool, default True
        If set to True, the source columns are dropped after being untokenized,
        and the resulting columns retain the names of the source columns.
        Otherwise, untokenized columns gain the suffix '_untok'.

    Example
    -------
        >>> import pandas as pd; import pdpipe as pdp;
        >>> data = [[3.2, ['Shake', 'and', 'bake!']]]
        >>> df = pd.DataFrame(data, [1], ['freq', 'content'])
        >>> untokenize_stage = pdp.UntokenizeWords('content')
        >>> untokenize_stage(df)
           freq          content
        1   3.2  Shake and bake!
    """

    def __init__(self, columns, drop=True, **kwargs):
        super().__init__(
            columns=columns,
            value_map=(lambda tokens: ' '.join(tokens)),
            drop=drop
        )

        # TODO add Description to make sure all columns are of "object" dtype


class RemoveStopwords(MapColVals):
    """A pipeline stage that removes stopwords from a tokenized list.

    Note: The nltk package must be installed for this pipeline stage to work.

    Parameters
    ----------
    langugae : str or array-like
        If a string is given, interpreted as the language of the stopwords, and
        should then be one of the languages supported by the NLTK Stopwords
        Corpus. If a list is given, it is assumed to be the list of stopwords
        to remove.
    columns : str or list-like
        Column names in the DataFrame from which to remove stopwords.
    drop : bool, default True
        If set to True, the source columns are dropped after stopword removal,
        and the resulting columns retain the names of the source columns.
        Otherwise, resulting columns gain the suffix '_nostop'.

    Example
    -------
        >> import pandas as pd; import pdpipe as pdp;
        >> data = [[3.2, ['kick', 'the', 'baby']]]
        >> df = pd.DataFrame(data, [1], ['freq', 'content'])
        >> remove_stopwords = pdp.RemoveStopwords('english', 'content')
        >> remove_stopwords(df)
           freq       content
        1   3.2  [kick, baby]
    """

    class _StopwordsRemover(object):
        def __init__(self, stopwords_list):
            self.stopwords_list = stopwords_list

        def __call__(self, word_list):
            return [w for w in word_list if w not in self.stopwords_list]

    @staticmethod
    def __stopwords_by_language(language):
        try:
            from nltk.corpus import stopwords
            return stopwords.words(language)
        except LookupError:  # pragma: no cover
            # try:
            #     nltk.data.find('corpora/stopwords')
            # except LookupError:  # pragma: no cover
            dpath = os.path.expanduser('~/nltk_data/corpora/stopwords')
            os.makedirs(dpath, exist_ok=True)
            nltk.download('stopwords')
            from nltk.corpus import stopwords
            return stopwords.words(language)

    def __init__(self, language, columns, drop=True, **kwargs):

        self._language = language

        if isinstance(language, str):
            self._stopwords_list = RemoveStopwords.__stopwords_by_language(
                language)
        elif isinstance(language, collections.Iterable):
            self._stopwords_list = list(language)
        else:
            raise TypeError("language parameter should be string or list!")

        self._stopwords_remover = RemoveStopwords._StopwordsRemover(
            self._stopwords_list)

        super().__init__(
            columns=columns,
            value_map=self._stopwords_remover,
            drop=drop
        )

        # TODO add Description to make sure all columns are of "object" dtype


class SnowballStem(MapColVals):
    """A pipeline stage that stems words in a list using the Snowball stemmer.

    Note: The nltk package must be installed for this pipeline stage to work.

    Parameters
    ----------
    stemmer_name : str
        The name of the Snowball stemmer to use. Should be one of the Snowball
        stemmers implemented by nltk. E.g. 'EnglishStemmer'.
    columns : str or list-like
        Column names in the DataFrame to stem tokens in.
    drop : bool, default True
        If set to True, the source columns are dropped after stemming, and the
        resulting columns retain the names of the source columns. Otherwise,
        resulting columns gain the suffix '_stem'.

    Example
    -------
        >>> import pandas as pd; import pdpipe as pdp;
        >>> data = [[3.2, ['kicking', 'boats']]]
        >>> df = pd.DataFrame(data, [1], ['freq', 'content'])
        >>> remove_stopwords = pdp.SnowballStem('EnglishStemmer', 'content')
        >>> remove_stopwords(df)
           freq       content
        1   3.2  [kick, boat]
    """

    class _TokenListStemmer(object):
        def __init__(self, stemmer):
            self.stemmer = stemmer

        def __call__(self, token_list):
            return [self.stemmer.stem(w) for w in token_list]

    @staticmethod
    def __stemmer_by_name(stemmer_name):
        snowball_module = importlib.import_module('nltk.stem.snowball')
        stemmer_cls = getattr(snowball_module, stemmer_name)
        return stemmer_cls()

    @staticmethod
    def __safe_stemmer_by_name(stemmer_name):
        try:
            return SnowballStem.__stemmer_by_name(stemmer_name)
        except LookupError:  # pragma: no cover
            dpath = os.path.expanduser('~/nltk_data/stemmers')
            os.makedirs(dpath, exist_ok=True)
            nltk.download('snowball_data')
            return SnowballStem.__stemmer_by_name(stemmer_name)

    def __init__(self, stemmer_name, columns, drop=True, **kwargs):

        self.stemmer_name = stemmer_name
        self.stemmer = SnowballStem.__safe_stemmer_by_name(stemmer_name)
        self.list_stemmer = SnowballStem._TokenListStemmer(self.stemmer)

        super().__init__(
            columns=columns,
            value_map=self.list_stemmer,
            drop=drop
        )


class DropRareTokens(_ColValSelection):
    """A pipeline stage that drop rare tokens from token lists.

    Note: The nltk package must be installed for this pipeline stage to work.

    Parameters
    ----------
    columns : str or list-like
        Column names in the DataFrame for which to drop rare words.
    threshold : int
        The rarity threshold to use. Only tokens appearing more than this
        number of times in a column will remain in token lists in that column.
    drop : bool, default True
        If set to True, the source columns are dropped after being transformed,
        and the resulting columns retain the names of the source columns.
        Otherwise, the new columns gain the suffix '_norare'.

    Example
    -------
        >>> import pandas as pd; import pdpipe as pdp;
        >>> data = [[7, ['a', 'a', 'b']], [3, ['b', 'c', 'd']]]
        >>> df = pd.DataFrame(data, columns=['num', 'chars'])
        >>> rare_dropper = pdp.DropRareTokens('chars', 1)
        >>> rare_dropper(df)
           num      chars
        0    7  [a, a, b]
        1    3        [b]
    """

    def __init__(self, columns, threshold, drop=True):

        super().__init__(
            columns=columns,
            values=threshold
        )

        self._drop = drop
        self._rare_removers = {}

    class _RareRemover(object):
        def __init__(self, rare_words):
            self.rare_words = rare_words

        def __call__(self, tokens):
            return [w for w in tokens if w not in self.rare_words]

    @staticmethod
    def __get_rare_remover(series, threshold):
        token_list = [item for sublist in series for item in sublist]
        freq_dist = nltk.FreqDist(token_list)
        freq_series = pd.DataFrame.from_dict(freq_dist, orient='index')[0]
        rare_words = freq_series[freq_series <= threshold]

        return DropRareTokens._RareRemover(rare_words)

    def transform(self, data, **kwargs):

        inter_df = data

        for colname in self._columns:

            source_col = data[colname]
            loc = data.columns.get_loc(colname) + 1
            new_name = colname + "_norare"

            if self._drop:
                inter_df = inter_df.drop(colname, axis=1)
                new_name = colname
                loc -= 1

            rare_remover = DropRareTokens.__get_rare_remover(
                source_col, self._values)

            self._rare_removers[colname] = rare_remover

            inter_df = out_of_place_col_insert(
                df=inter_df,
                series=source_col.map(rare_remover),
                loc=loc,
                column_name=new_name)

        return inter_df
