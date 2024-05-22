from datetime import timedelta
import json
import re
import subprocess
from urllib.parse import urlparse, urljoin, quote as urlquote
import warnings
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup
from lxml import html
from pyld import jsonld
import requests_cache

from pprint import pprint


def get_doi_id(doi):
    """Get the id part from a doi

    A doi is canonically reported as a url, but "doi:" or no prefix
    form are also used. This tries to isolate the id part.

    """
    if doi.lower().startswith('http'):
        parsed = urlparse(doi)
        id = parsed.path.lstrip('/')
    elif doi.lower().startswith("doi:"):
        id = doi[4:]
    else:
        id = doi
    return id


CROSSREF_AUTHOR = {
    "schema": "https://schema.org/",
    "ORCID": "https://purl.obolibrary.org/IAO_0000708",
    "given": "schema:givenName",
    "family": "schema:familyName",
    "affiliation": "schema:affiliation",
    "name": "schema:name",
    #"suffix", "authenticated-orcid", "prefix", "sequence"
}

CAT_AUTHOR = {
    "givenName": "https://schema.org/givenName",
    "familyName": "https://schema.org/familyName",
    "name": "https://schema.org/name",
    "email": "https://schema.org/email",
    "orcid": "https://purl.obolibrary.org/IAO_0000708",  # -> identifiers
    "honorificSuffix": "https://schema.org/honorificSuffix"
}


def author_from_csl(author):
    """Translate (parts of) author from CSL to catalog

    Currently only takes name components that are present (note: a
    person has givenName and familyName, an organization just name).

    """
    d = {
        "givenName": author.get("given"),
        "familyName": author.get("family"),
        "name": author.get("name"),
    }
    return {k: v for k, v in d.items() if v is not None}

def dehtmlize(s):
    """Remove html formatting from string"""
    return html.fromstring(s).text_content()

def query_doi_org(doi, session_name="tabby-utils-queries", useragent=None):
    """Perform a doi query at doi.org

    Queries doi.org about a given doi, using content negotiation to
    request CSL json. This should get redirected to crossref,
    datacite, or medra.

    See: https://citation.crosscite.org/docs.html

    """

    session = requests_cache.CachedSession(session_name, use_cache_dir=True)

    headers = {"Accept": "application/vnd.citationstyles.csl+json"}
    if useragent is not None:
        headers["User-Agent"] = useragent

    r = session.get(
        url=f"https://doi.org/{doi}",
        headers=headers,
    )

    if not r.ok:
        return None

    res = r.json()
    pub = {
        "type": res.get("type"),  # prob. journal-article  # required
        "title": dehtmlize(res.get("title")),  # required
        "doi": res.get("DOI"),  # required
        # earliest of published-[print,online]
        "datePublished": res.get("issued", {}).get("date-parts", [[None]])[0][0],
        "publicationOutlet": res.get("container-title"),
        "authors": [
            author_from_csl(x) for x in res.get("author")
        ],
    }

    if not pub["doi"].startswith("http"):
        # pretty sure it's always without doi.org part
        pub["doi"] = "https://doi.org/" + pub["doi"]

    if pub["publicationOutlet"] == []:
        # seen e.g. for Research Square preprints
        pub.pop("publicationOutlet")

    return pub


def query_agency(doi, session_name="tabby-utils-queries"):
    """Query doi.org about registration agency"""

    session = requests_cache.CachedSession(session_name, use_cache_dir=True)

    if doi.startswith("http"):
        r = session.get(doi.replace("doi.org/", "doi.org/ra/"))
    else:
        r = session.get(f"https://doi.org/ra/{doi}")

    return r.json()[0]["RA"] if r.ok else None


def query_crossref_xml(doi, session_name="tabby-utils-queries", email=None):
    """Perform a DOI to metadata query in Crossref's XML API"""

    session = requests_cache.CachedSession(session_name, use_cache_dir=True)

    doi = doi.replace("https://doi.org/", "")  # we want plain doi

    if email is None:
        # email is needed to query the API, we fill in from git config
        # we can reasonably expect one to be present, so not handling errors
        sp_git = subprocess.run(
            ["git", "config", "user.email"], capture_output=True, text=True
        )
        email = sp_git.stdout.rstrip()

    r = session.get(
        "https://doi.crossref.org/servlet/query",
        params={"pid": email, "format": "unixref", "id": doi},
    )

    if not r.ok:
        return None

    root = ET.fromstring(r.text)

    # pick returned element and translate to be catalog-compatible
    elem = root.find("doi_record/crossref/")  # / at the end means child
    if elem.tag == "journal":
        return unixref_journal(elem)
    else:
        raise NotImplementedError(f"No parser implemented for {elem.tag}")

    return root


def unixref_journal(elem):
    """Translate journal (article) from UNIXREF to catalog

    Takes XML journal element (which combines journal_metadata and
    journal_article) and returns a catalog-compatible dict. Uses
    XPaths to select the required elements.

    See:
    https://data.crossref.org/reports/help/schema_doc/unixref1.1/unixref1.1.html

    """

    publication = {"type": "journal-article"}

    if (title := elem.find("journal_article/titles/title")) is not None:
        publication["title"] = title.text

    if (doi := elem.find("journal_article/doi_data/doi")) is not None:
        # this should be guaranteed to exist
        if doi.text.startswith("http"):
            publication["doi"] = doi.text
        else:
            publication["doi"] = f"https://doi.org/{doi.text}"

    # let's try to favour print date, otherwise take first available
    datePublished = elem.find(
        "journal_article/publication_date[@media_type='print']/year"
    )
    if datePublished is None:
        datePublished = elem.find("journal_article/publication_date/year")

    if datePublished is not None:
        # this should be guaranteed to exist
        publication["datePublished"] = datePublished.text

    if (
        publicationOutlet := elem.find("journal/journal_metadata/full_title")
    ) is not None:
        publication["publicationOutlet"] = publicationOutlet.text

    contributors = elem.find("journal_article/contributors")
    if contributors is not None:
        authors = []
        for c in contributors:
            if c.tag == "person_name":
                author = {}
                if (givenName := c.find("given_name")) is not None:
                    author["givenName"] = givenName.text
                if (familyName := c.find("surname")) is not None:
                    author["familyName"] = familyName.text
                if (honorificSuffix := c.find("suffix")) is not None:
                    author["honorificSuffix"] = honorificSuffix.text
                if (orcid := c.find("ORCID")) is not None:
                    id_part = re.search(
                        "0000-000(1-[5-9]|2-[0-9]|3-[0-4])[0-9]{3}-[0-9]{3}[0-9X]",
                        orcid.text,
                    ).group()
                    author["identifiers"] = [{"type": "ORCID", "identifier": id_part}]
            elif c.tag == "organization":
                # simple type, retrieve text directly from element
                author = {"name": c.text}
            elif c.tag == "anonymous":
                # anonymous contributor can still have afffiliation :D
                author = {"name": "anonymous"}
            else:
                continue
            authors.append(author)


        publication["authors"] = authors

    return publication


def ols_lookup(term, session, iri_prefix="http://purl.obolibrary.org/obo/"):
    """Look up a term in OLS API

    Takes a term like like UBERON:0013702. Assumes that the part
    before the colon is the ontology name, and the IRI can be formed
    by replacing ":" with "_". Queries www.ebi.ac.uk/ols4 api.

    Returns a json with response content, or None.

    API docs: https://www.ebi.ac.uk/ols4/help

    """

    api = "http://www.ebi.ac.uk/ols4/api/ontologies"

    ontology = term.split(":")[0].lower()
    iri = urlquote(urlquote(urljoin(iri_prefix, term.replace(":", "_")), safe=""))
    url = f"{api}/{ontology}/terms/{iri}"

    r = session.get(url, headers={"Accept": "application/json"})

    if r.status_code != 200:
        warnings.warn(f"OLS lookup for {term} returned {r.status_code}", stacklevel=2)
        return None

    return r.json()


def repr_ncbitaxon(ols_response, default=None):
    """Turn OLS api response to OpenMINDS Species dict.

    Looks up specific keys in the response.

    """
    if ols_response is None:
        # 400 / 404 response, return term unchanged
        return default

    # Create an OpenMinds species object
    # https://openminds-documentation.readthedocs.io/en/latest/specifications/controlledTerms/species.html
    species = {
        "@type": "https://openminds.ebrains.eu/controlledTerms/Species",
        "name": ols_response.get("label"),
        "preferredOntologyIdentifier": ols_response.get("iri"),
    }

    # find genbank common name that is an exact synonym
    obo_synonym = ols_response.get("obo_synonym")
    if obo_synonym is None:
        # key present but value empty, or key missing
        obo_synonym = []
    if isinstance(obo_synonym, dict):
        obo_synonym = [obo_synonym]
    for s in obo_synonym:
        if s.get('scope') == 'hasExactSynonym' and s.get('type') == 'genbank common name':
            species["synonym"] = s.get("name")
            break

    return species


def repr_uberon(ols_response, default=None):
    """Turn OLS api response to OpenMINDS Species dict.

    Looks up specific keys in the response.

    """

    if ols_response is None:
        return default

    UBERONParcellation = {
        "@type": "https://openminds.ebrains.eu/controlledTerms/UBERONParcellation",
        "name": ols_response.get("label"),
        "preferredOntologyIdentifier": ols_response.get("iri"),
    }

    return UBERONParcellation


def process_ols_term(term, filter_func, session_name="tabby-utils-queries"):
    """Query OLS api and return nice representations

    Runs an OLS API query for the given term and applies filter_func
    to its result. Accepts single term, list of terms, or None, and
    returns the same type. Uses requests_cache session to cache
    responses.

    """
    session = requests_cache.CachedSession(session_name, use_cache_dir=True)

    if isinstance(term, list):
        return [filter_func(ols_lookup(t, session), t) for t in term]
    elif isinstance(term, str):
        return filter_func(ols_lookup(term, session), term)
    else:
        return None


def query_cordis(cordis_url, session_name="tabby-utils-queries"):
    """Get grant name and acronym from CORDIS

    The funder and identifier can be filled in from elsewhere

    """

    session = requests_cache.CachedSession(session_name, use_cache_dir=True)

    r = session.get(
        cordis_url,
        params={"format": "xml"},
    )

    if not r.ok:
        return None

    root = ET.fromstring(r.text)

    xmlns = {"cordis": "http://cordis.europa.eu"}

    grant = {
        "name": root.find("cordis:title", xmlns).text,
        "alternateName": root.find("cordis:acronym", xmlns).text,
    }

    return grant


def parse_gepris(gepris_url, session_name="tabby-utils-queries"):
    """Get grant name from GEPRIS

    Needs to parse html because there is no structured metadata query
    (that I know of).

    """

    session = requests_cache.CachedSession(session_name, use_cache_dir=True)
    r = session.get(
        gepris_url,
        params={"language": "en"},
    )

    if not r.ok:
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    name = soup.find("h1", class_=lambda x: x != "hidden").get_text(strip=True)
    name = " ".join(name.split())  # get rid of nbsp, multiple spaces, newlines

    grant = {"name": name}

    return grant
