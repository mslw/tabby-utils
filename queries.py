from datetime import timedelta
import json
from urllib.parse import urlparse, urljoin, quote as urlquote
import warnings
import xml.etree.ElementTree as ET

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


def query_doi_org(doi, session_name="query_cache", useragent=None):
    """Perform a doi query at doi.org

    Queries doi.org about a given doi, using content negotiation to
    request CSL json. This should get redirected to crossref,
    datacite, or medra.

    See: https://citation.crosscite.org/docs.html

    """

    session = requests_cache.CachedSession(session_name)

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
        "title": res.get("title"),  # required
        "doi": res.get("DOI"),  # required
        # earliest of published-[print,online]
        "datePublished": res.get("issued", {}).get("date-parts", [[None]])[0][0],
        "publicationOutlet": res.get("container-title"),
        "authors": [
            {"givenName": x.get("given"), "familyName": x.get("family")}
            for x in res.get("author")
        ],
    }

    if not pub["doi"].startswith("http"):
        # pretty sure it's always without doi.org part
        pub["doi"] = "https://doi.org/" + pub["doi"]

    return pub


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


def process_ols_term(term, filter_func, session_name="query_cache"):
    """Query OLS api and return nice representations

    Runs an OLS API query for the given term and applies filter_func
    to its result. Accepts single term, list of terms, or None, and
    returns the same type. Uses requests_cache session to cache
    responses.

    """
    session = requests_cache.CachedSession(session_name)

    if isinstance(term, list):
        return [filter_func(ols_lookup(t, session), t) for t in term]
    elif isinstance(term, str):
        return filter_func(ols_lookup(term, session), term)
    else:
        return None
