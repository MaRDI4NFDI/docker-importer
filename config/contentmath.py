from wikibaseintegrator.datatypes.string import String


class ContentMath(String):
    """
    Implements the Wikibase data type 'math' for mathematical formula in TEX format
    """
    DTYPE = 'contentmath'