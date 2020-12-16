import pytest

from crawler.parser import AnchorTagParser
from crawler.parser import HyperlinkReference


def make_html(body: str) -> str:
    """make a html doc by passing data to appear in <body> tag"""
    return f"<html><head></head><body>{body}</body></html>"


def make_a_tag(path: str) -> str:
    """make a <a> tag with path as HREF value"""
    return f'<a href="{path}">another link</a>'


def make_a_tags(paths: list) -> str:
    """make multiple <a> tags (via `make_a_tag`) seperated by <br> tags"""
    return "<br>".join([make_a_tag(path) for path in paths])


@pytest.mark.parametrize(
    "input_link_and_output_result",
    [
        ("/", "/"),
        (".", "/"),
        ("example", "/example"),
        ("/example", "/example"),
        ("www.example.html", "/www.example.html"),
        ("#hello", "/#hello"),
        ("/#hello", "/#hello"),
        ("example#hello", "/example#hello"),
        ("/example#hello", "/example#hello"),
        ("?hello=world", "/?hello=world"),
        ("/?hello=world", "/?hello=world"),
        ("https://www.example.com/", "https://www.example.com/"),
        ("https://www.example.com.", "https://www.example.com/"),
        ("https://www.example.com/example", "https://www.example.com/example"),
        ("https://www.example.com#hello", "https://www.example.com/#hello"),
        ("https://www.example.com/#hello", "https://www.example.com/#hello"),
        (
            "https://www.example.com/example#hello",
            "https://www.example.com/example#hello",
        ),
        ("https://www.example.com?hello=world", "https://www.example.com/?hello=world"),
        (
            "https://www.example.com/?hello=world",
            "https://www.example.com/?hello=world",
        ),
    ],
)
def test_hyper_link_reference(input_link_and_output_result):
    input_link, output_result = input_link_and_output_result
    href = HyperlinkReference(input_link)
    assert href == output_result


@pytest.mark.parametrize(
    "input_link_and_output_result",
    [
        ("/", False),
        (".", False),
        ("example", False),
        ("/example", False),
        ("www.example.html", False),
        ("#hello", False),
        ("/#hello", False),
        ("example#hello", False),
        ("/example#hello", False),
        ("?hello=world", False),
        ("/?hello=world", False),
        ("https://www.example.com/", True),
        ("https://www.example.com.", True),
        ("https://www.example.com/example", True),
        ("https://www.example.com#hello", True),
        ("https://www.example.com/#hello", True),
        ("https://www.example.com/example#hello", True),
        ("https://www.example.com?hello=world", True),
        ("https://www.example.com/?hello=world", True),
    ],
)
def test_hyper_link_is_absolute_or_relative(input_link_and_output_result):
    input_link, output_result = input_link_and_output_result
    href = HyperlinkReference(input_link)
    assert href.is_absolute is output_result
    assert href.is_relative is not output_result


@pytest.mark.parametrize(
    "input_link_and_output_result",
    [
        ("/", "/"),
        (".", "/"),
        ("example", "/example"),
        ("/example", "/example"),
        ("www.example.html", "/www.example.html"),
        ("#hello", "/#hello"),
        ("/#hello", "/#hello"),
        ("example#hello", "/example#hello"),
        ("/example#hello", "/example#hello"),
        ("?hello=world", "/?hello=world"),
        ("/?hello=world", "/?hello=world"),
    ],
)
def test_hyper_link_join_with_relative_links(input_link_and_output_result):
    input_link, output_result = input_link_and_output_result
    href = HyperlinkReference(input_link)
    domain = "https://helloworld.com"
    assert href.join(domain) == domain + output_result


@pytest.mark.parametrize(
    "input_link_and_output_result",
    [
        ("https://www.example.com/", "https://www.example.com/"),
        ("https://www.example.com.", "https://www.example.com/"),
        ("https://www.example.com/example", "https://www.example.com/example"),
        ("https://www.example.com#hello", "https://www.example.com/#hello"),
        ("https://www.example.com/#hello", "https://www.example.com/#hello"),
        (
            "https://www.example.com/example#hello",
            "https://www.example.com/example#hello",
        ),
        ("https://www.example.com?hello=world", "https://www.example.com/?hello=world"),
        (
            "https://www.example.com/?hello=world",
            "https://www.example.com/?hello=world",
        ),
    ],
)
def test_hyper_link_join_with_absolute_links(input_link_and_output_result):
    input_link, output_result = input_link_and_output_result
    href = HyperlinkReference(input_link)
    domain = "https://helloworld.com"
    assert href.join(domain) == output_result


@pytest.mark.parametrize(
    "link",
    [
        "https://example.com",
        "http://example.com",
        "mailto://example.com",
        "//example.com",
        "/",
        ".",
        "example",
        "example.html",
        "www.example.html",
        "../example.html",
        "#hello",
        "?hello=world",
        ".git",
        "/example",
        "/example.html",
        "/example#hello",
        "?hello=world",
        "/example?hello=world&world=hello",
    ],
)
def test_anchor_tag_parser_single_link(link):
    html, href = make_html(make_a_tag(link)), HyperlinkReference(link)
    parser = AnchorTagParser()
    parser.feed(html)
    assert parser.found_links == [href]


@pytest.mark.parametrize(
    "links",
    [
        ["https://example.com", "http://example.com"],
        ["http://example.com", "mailto://example.com", "//example.com"],
        ["/", ".", "example", "example.html"],
        ["www.example.html", "../example.html", "#hello", "?hello=world", ".git"],
        [
            "https://example.com",
            "/example",
            "/example.html",
            "/example#hello",
            "?hello=world",
            "/example?hello=world&world=hello",
        ],
    ],
)
def test_anchor_tag_parser_multiple_links_no_duplicates(links):
    html, hrefs = (
        make_html(make_a_tags(links)),
        [HyperlinkReference(link) for link in links],
    )
    parser = AnchorTagParser()
    parser.feed(html)
    assert parser.found_links == hrefs


@pytest.mark.parametrize(
    "links",
    [
        [
            "https://example.com",
            "http://example.com",
            "/example",
            "?hello=world",
            "/example?hello=world&world=hello",
        ],
        [
            "/hello-world",
            "http://example.com",
            "mailto://example.com",
            "//example.com",
            "/hello-world",
        ],
        [
            "https://example.com",
            "https://example.com",
            "#hello",
            "#hello",
            "?hello=world",
        ],
    ],
)
def test_anchor_tag_parser_multiple_links_with_duplicates(links):
    html, hrefs = (
        make_html(make_a_tags(links)),
        [HyperlinkReference(link) for link in links],
    )
    parser = AnchorTagParser()
    parser.feed(html)
    assert parser.found_links == hrefs