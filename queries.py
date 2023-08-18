from datetime import timedelta
import json
from urllib.parse import urlparse
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



session = requests_cache.CachedSession('query_cache')

email = "m.szczepanik@fz-juelich.de"
doi = "10.14454/FXWS-0523"  # datacite

# https://www.crossref.org/documentation/retrieve-metadata/xml-api/doi-to-metadata-query/
# r = session.get(
#     #url=f"https://doi.crossref.org/servlet/query?pid={email}&format=unixref&id={doi}",
#     #url = f"https://api.crossref.org/works/{doi}/agency?mailto={email}",
#     url = f"https://api.crossref.org/works/{doi}?mailto={email}",
#     expire_after=timedelta(hours=1)
# )


# r = session.get(
#         url = f"https://api.datacite.org/dois/{doi}",
#         expire_after=timedelta(hours=1)
#     )

# d = json.loads(r.text)
# data = d.get("data")
# attrs = data.get("attributes")

# doi = data.get("id")

# title = attrs.get("titles")[0].get("title")



def query_crossref(doi, session, email="m.szczepanik@fz-juelich.de"):

    r = session.get(
        url = f"https://api.crossref.org/works/{doi}?mailto={email}",
        expire_after=timedelta(hours=1)
    )

    if r.status_code != 200:
        return None

    d = json.loads(r.text)
    msg = d['message']

    pub = {
        "type": msg.get('type'),  # prob. journal-article  # required
        "title": msg.get('title')[0], # required
        "doi": msg.get('DOI'), # 10.nnnn/...  # required
        "datePublished": msg.get('issued', {}).get('date-parts', [[None]])[0][0],  # earliest of published-[print,online]
        "publicationOutlet": msg.get('container-title', [None])[0] # not required
    }

    authors = []
    for a in msg.get('author'):
        ca = jsonld.compact(a, ctx=CAT_AUTHOR, options={'expandContext': CROSSREF_AUTHOR})
        # drop @context and keys not defined for catalog
        author = {k:v for k,v in ca.items() if k in CAT_AUTHOR.keys()}
        # fold in orcid (see load_tabby.process_author)
        # see load_tabby:process_authors.py
        if orcid := author.pop("orcid", False):
            author["identifiers"] = [
                {"name": "ORCID", "identifier": orcid},
        ]
        # TODO: e-mail is required in the catalog shema dshgafhfadasfhdsgjfgasdjfgasdj!!!
        authors.append(author)

    pub["authors"] = authors

    return pub

# pprint(query_crossref("10.1371/journal.pone.0090081", session, email))
