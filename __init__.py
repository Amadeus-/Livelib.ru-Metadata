# -*- coding: utf-8 -*-
"""
Livelib.ru Metadata Source Plugin for Calibre

Fetches book metadata from livelib.ru, a comprehensive Russian book database.
Useful for Russian-language books and translations.
"""
__author__ = 'Amadeus'

import re
import json
import time
import lxml.html as html
import urllib.parse

from calibre.ebooks.metadata.book.base import Metadata
from calibre.ebooks.metadata.sources.base import Source


class LivelibMetadataSourcePlugin(Source):
    name = 'Livelib.ru'
    version = (1, 0, 0)
    author = 'Amadeus'
    description = 'Downloads metadata from livelib.ru, a Russian book database. ' \
                  'Useful for Russian books and translations to Russian.'

    capabilities = frozenset(('identify', 'cover'))
    supported_platforms = ('windows', 'osx', 'linux')
    touched_fields = frozenset([
        'title', 'authors', 'identifier:livelib', 'identifier:isbn',
        'tags', 'series', 'publisher', 'comments', 'rating'
    ])
    has_html_comments = False
    can_get_multiple_covers = False
    cached_cover_url_is_reliable = True

    BASE_URL = 'https://www.livelib.ru'

    # Minimum time between requests (seconds)
    MIN_REQUEST_INTERVAL = 0.5

    def get_book_url(self, identifiers):
        """Return URL for book page given identifiers."""
        livelib_id = identifiers.get('livelib')
        if livelib_id:
            return f'{self.BASE_URL}/book/{livelib_id}'
        return None

    def id_from_url(self, url):
        """Extract livelib ID from URL."""
        match = re.search(r'/book/(\d+)', url)
        if match:
            return match.group(1)
        return None

    def get_cached_cover_url(self, identifiers):
        """Return cached cover URL if available."""
        livelib_id = identifiers.get('livelib')
        if livelib_id:
            # Will be populated during identify
            return self.cached_identifier_to_cover_url(f'livelib:{livelib_id}')
        return None

    def _fetch_page(self, url, log, timeout=30):
        """Fetch a page using Calibre's browser."""
        try:
            time.sleep(self.MIN_REQUEST_INTERVAL)
            br = self.browser
            response = br.open_novisit(url, timeout=timeout)
            return response.read()
        except Exception as e:
            log.exception(f'Error fetching {url}: {e}')
            return None

    def _extract_json_ld(self, root, log):
        """Extract JSON-LD structured data from page."""
        scripts = root.xpath('//script[@type="application/ld+json"]/text()')
        for script in scripts:
            try:
                data = json.loads(script)
                if isinstance(data, dict) and data.get('@type') == 'Book':
                    return data
            except json.JSONDecodeError:
                continue
        return None

    def parse_book_page(self, book_url, log, timeout=30):
        """Parse a book page and extract metadata."""
        log.info(f'Fetching book page: {book_url}')

        raw_html = self._fetch_page(book_url, log, timeout)
        if not raw_html:
            return None

        try:
            root = html.fromstring(raw_html)
        except Exception as e:
            log.exception(f'Error parsing HTML: {e}')
            return None

        # Try JSON-LD first (most reliable)
        json_ld = self._extract_json_ld(root, log)

        title = ''
        authors = []
        isbn = None
        publisher = None
        genres = []
        series = None
        series_index = None
        description = None
        rating = None
        cover_url = None
        pub_year = None

        # Extract from JSON-LD if available
        if json_ld:
            log.info('Found JSON-LD data')
            title = json_ld.get('name', '')
            log.info(f'Title from JSON-LD: {title}')

            # Author
            author_data = json_ld.get('author')
            if author_data:
                if isinstance(author_data, dict):
                    author_name = author_data.get('name')
                    if author_name:
                        authors.append(author_name)
                elif isinstance(author_data, list):
                    for a in author_data:
                        if isinstance(a, dict) and a.get('name'):
                            authors.append(a['name'])
            log.info(f'Authors from JSON-LD: {authors}')

            isbn = json_ld.get('isbn')
            if isbn:
                log.info(f'ISBN from JSON-LD: {isbn}')

            publisher_data = json_ld.get('publisher')
            if publisher_data and isinstance(publisher_data, dict):
                publisher = publisher_data.get('name')
                log.info(f'Publisher from JSON-LD: {publisher}')

            genre = json_ld.get('genre')
            if genre:
                if isinstance(genre, str):
                    genres.append(genre)
                elif isinstance(genre, list):
                    genres.extend(genre)
                log.info(f'Genres from JSON-LD: {genres}')

            description = json_ld.get('description')

            # Rating
            rating_data = json_ld.get('aggregateRating')
            if rating_data and isinstance(rating_data, dict):
                try:
                    rating = float(rating_data.get('ratingValue', 0))
                    log.info(f'Rating from JSON-LD: {rating}')
                except (ValueError, TypeError):
                    pass

            # Cover image
            cover_url = json_ld.get('image')
            if cover_url:
                log.info(f'Cover URL from JSON-LD: {cover_url}')

        # Fallback: extract from HTML if JSON-LD missing data
        if not title:
            h1 = root.xpath('//h1/text()')
            if h1:
                title = h1[0].strip()
                log.info(f'Title from H1: {title}')

        if not authors:
            author_links = root.xpath('//a[contains(@href, "/author/")]/text()')
            for author in author_links[:3]:  # Limit to first 3 to avoid sidebar
                author = author.strip()
                if author and author not in authors:
                    authors.append(author)
            log.info(f'Authors from HTML: {authors}')

        # Series (not in JSON-LD, must parse HTML)
        series_links = root.xpath('//a[contains(@href, "/series/") or contains(@href, "/pubseries/")]')
        for link in series_links:
            series_text = link.text_content().strip()
            if series_text:
                series = series_text
                log.info(f'Series from HTML: {series}')
                # Try to find series index
                tail = link.tail
                if tail:
                    index_match = re.search(r'#(\d+)', tail)
                    if index_match:
                        series_index = int(index_match.group(1))
                        log.info(f'Series index: {series_index}')
                break

        # Additional genres from HTML
        if not genres:
            genre_links = root.xpath('//a[contains(@href, "/genre/")]/text()')
            for genre in genre_links:
                genre = genre.strip()
                if genre and genre not in genres:
                    genres.append(genre)
            log.info(f'Genres from HTML: {genres}')

        # Publication year from HTML
        year_match = root.xpath('//*[contains(text(), "Год издания")]/following-sibling::*/text()')
        if year_match:
            try:
                pub_year = int(year_match[0].strip())
                log.info(f'Publication year: {pub_year}')
            except (ValueError, IndexError):
                pass

        # Extract book ID from URL
        book_id = self.id_from_url(book_url)

        if not title or not authors:
            log.info('Missing title or authors, skipping')
            return None

        # Build metadata object
        mi = Metadata(title, authors)

        if book_id:
            mi.set_identifier('livelib', book_id)

        if isbn:
            mi.set_identifier('isbn', isbn)

        if genres:
            mi.tags = genres

        if series:
            mi.series = series
            if series_index:
                mi.series_index = series_index

        if publisher:
            mi.publisher = publisher

        if description:
            mi.comments = description

        if rating:
            # Calibre uses 0-10 scale, Livelib uses 0-5
            mi.rating = rating * 2

        if pub_year:
            from calibre.utils.date import parse_only_date
            try:
                mi.pubdate = parse_only_date(str(pub_year))
            except:
                pass

        # Cache cover URL
        if cover_url and book_id:
            self.cache_identifier_to_cover_url(f'livelib:{book_id}', cover_url)

        log.info(f'Metadata extracted: {mi.title} by {mi.authors}')
        return mi

    def identify(self, log, result_queue, abort, title=None, authors=None,
                 identifiers=None, timeout=30):
        """Identify books matching the given criteria."""
        log.info('Livelib.ru identification started...')
        identifiers = identifiers or {}

        # Check if we have a livelib ID already
        livelib_id = identifiers.get('livelib')
        if livelib_id:
            book_url = f'{self.BASE_URL}/book/{livelib_id}'
            log.info(f'Using existing livelib ID: {livelib_id}')
            mi = self.parse_book_page(book_url, log, timeout)
            if mi:
                self.clean_downloaded_metadata(mi)
                result_queue.put(mi)
            return

        if not title:
            log.info('No title provided, cannot search')
            return

        # Build search query
        title_tokens = ' '.join(self.get_title_tokens(title))
        author_tokens = ''
        if authors:
            author_tokens = ' '.join(self.get_author_tokens(authors, only_first_author=True))

        # Combine title and author for better search
        search_query = title_tokens
        if author_tokens:
            search_query = f'{title_tokens} {author_tokens}'

        log.info(f'Searching for: "{search_query}"')

        # URL encode the query
        encoded_query = urllib.parse.quote(search_query.encode('utf8'))
        search_url = f'{self.BASE_URL}/find/books/{encoded_query}'

        log.info(f'Search URL: {search_url}')

        raw_html = self._fetch_page(search_url, log, timeout)
        if not raw_html:
            log.info('Failed to fetch search results')
            return

        try:
            root = html.fromstring(raw_html)
        except Exception as e:
            log.exception(f'Error parsing search results: {e}')
            return

        # Find book links in search results
        book_links = root.xpath('//a[contains(@href, "/book/")]/@href')
        book_links = list(dict.fromkeys(book_links))  # Remove duplicates

        log.info(f'Found {len(book_links)} book links')

        if not book_links:
            log.info('No books found in search results')
            return

        # Try to find best match
        title_lower = title_tokens.lower()
        author_lower = author_tokens.lower() if author_tokens else ''

        best_match = None
        for href in book_links[:20]:  # Check first 20
            if abort.is_set():
                return

            # Get the link element to check its text
            link_elems = root.xpath(f'//a[@href="{href}"]')
            if not link_elems:
                continue

            link_text = link_elems[0].text_content().strip().lower()

            # Check if title matches
            if title_lower in link_text or link_text in title_lower:
                log.info(f'Title match: {link_text}')

                # If we have author, check for author match
                if author_lower:
                    # Look in parent container for author
                    parent = link_elems[0].getparent()
                    if parent is not None:
                        parent_text = parent.text_content().lower()
                        if author_lower in parent_text:
                            log.info('Author also matches')
                            best_match = href
                            break
                else:
                    best_match = href
                    break

        # Fall back to first result if no exact match
        if not best_match and book_links:
            best_match = book_links[0]
            log.info(f'No exact match, using first result: {best_match}')

        if best_match:
            # Convert relative URL to absolute
            if not best_match.startswith('http'):
                best_match = f'{self.BASE_URL}{best_match}'

            mi = self.parse_book_page(best_match, log, timeout)
            if mi:
                if abort.is_set():
                    return
                self.clean_downloaded_metadata(mi)
                result_queue.put(mi)

    def download_cover(self, log, result_queue, abort, title=None, authors=None,
                       identifiers=None, timeout=30, get_best_cover=False):
        """Download cover image for the book."""
        log.info('Livelib.ru cover download started...')
        identifiers = identifiers or {}

        # Check cached cover URL first
        livelib_id = identifiers.get('livelib')
        cover_url = None

        if livelib_id:
            cover_url = self.cached_identifier_to_cover_url(f'livelib:{livelib_id}')

        if not cover_url and livelib_id:
            # Fetch book page to get cover URL
            book_url = f'{self.BASE_URL}/book/{livelib_id}'
            raw_html = self._fetch_page(book_url, log, timeout)
            if raw_html:
                try:
                    root = html.fromstring(raw_html)
                    json_ld = self._extract_json_ld(root, log)
                    if json_ld:
                        cover_url = json_ld.get('image')
                except:
                    pass

        if not cover_url:
            log.info('No cover URL found')
            return

        log.info(f'Downloading cover from: {cover_url}')

        try:
            time.sleep(self.MIN_REQUEST_INTERVAL)
            br = self.browser
            response = br.open_novisit(cover_url, timeout=timeout)
            cover_data = response.read()

            if cover_data:
                result_queue.put((self, cover_data))
                log.info('Cover downloaded successfully')
        except Exception as e:
            log.exception(f'Error downloading cover: {e}')


if __name__ == '__main__':
    # Test code - run with: calibre-debug -e __init__.py
    from calibre.ebooks.metadata.sources.test import (
        test_identify_plugin, title_test, authors_test
    )

    test_identify_plugin(LivelibMetadataSourcePlugin.name, [
        (
            {
                'title': 'Ангел пролетел',
                'authors': ['Татьяна Устинова']
            },
            [
                title_test('Ангел пролетел', exact=False),
                authors_test(['Татьяна Устинова'])
            ]
        ),
    ])
