project = "En Banc"
author = "Free Law Project"

extensions = [
    "sphinxcontrib.mermaid",
    "sphinx_immaterial",
]

html_theme = "sphinx_immaterial"
html_theme_options = {
    "font": False,
    "features": [
        "navigation.expand",
        "navigation.top",
        "toc.follow",
    ],
}
