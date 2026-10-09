"""Portal datatypes unknown to WikibaseIntegrator (mathml is defined by mardiclient)."""

from wikibaseintegrator.datatypes import String


class ContentMath(String):
    DTYPE = "contentmath"
