from datetime import datetime
import json
from pathlib import Path
from pprint import pprint
import uuid
from urllib.parse import urlparse

from datalad_tabby.io import load_tabby
from datalad.api import catalog_add, catalog_remove, catalog_set, catalog_validate
from datalad.support.exceptions import IncompleteResultsError

from pyld import jsonld


def get_dataset_id(input, config):
    """"""
    # consult config for custom ID selection,
    # otherwise take plain standard field
    fmt = config.get("dataset_id_fmt", "{dataset_id}")
    # instantiate raw ID string
    raw_id = fmt.format(**input)
    # now turn into UUID deterministically
    return str(
        uuid.uuid5(
            uuid.uuid5(uuid.NAMESPACE_DNS, "datalad.org"),
            raw_id,
        )
    )


def get_metadata_source():
    """Create metadata_sources dict required by catalog schema"""
    source = {
        "key_source_map": {},
        "sources": [
            {
                "source_name": "tabby",
                "source_version": "0.1.0",
                "source_time": datetime.now().timestamp(),
                # "agent_email": get_gitconfig("user.name"),
                # "agent_name": get_gitconfig("user.email"),
            }
        ],
    }
    return source


def process_id(name):
    """Mint a deterministic uuid

    note: name alone is not enough
    """

    return str(
        uuid.uuid5(
            uuid.uuid5(uuid.NAMESPACE_DNS, "datalad.org"),
            name,
        )
    )


def process_author(author):
    known_keys = [
        "name",
        "email",
        "identifiers",
        "givenName",
        "familyName",
        "honorificSuffix",
    ]
    # pop orcid key and rewrite as list of {name, identifier}
    if orcid := author.pop("orcid", False):
        author["identifiers"] = [
            {"name": "ORCID", "identifier": orcid},
        ]
    # drop not-needed keys (like @type)
    return {k: author.get(k) for k in known_keys if author.get(k, False)}


def process_authors(authors):
    if authors is None:
        return None
    if type(authors) is dict:
        return process_author(a)
    return [process_author(a) for a in authors]


def process_license(license):
    """Convert license to catalog-schema object

    Catalog schema expects name & url. We can reasonably expect
    schema:license to be a URL, but won't hurt checking. But what
    about the name?

    """
    parsed_url = urlparse(license)
    if parsed_url.scheme != "" and parsed_url.netloc != "":
        # looks like a URL
        pass

    # do the least work, for now
    return {"name": license, "url": license}

def process_publications(publications):
    """Convert publication to catalog-schema object

    Catalog schema expects title, doi, datePublished,
    publicationOutlet, & authors. Our tabby spec only makes "citation"
    required, and allows doi, url, and datePublished (if doi is given,
    citation becomes optional).

    Best thing we can do in the citation-only case is to squeeze
    citation into title (big text that's displayed).

    When DOI is given, we can look it up to get all fields, and it's
    our artistic license whether citation should take precedence or
    not.

    """
    if type(publications) is dict:
        publications = [publications]

    res = []
    for publication in publications:
        citation = publication.pop("citation", None)

        if citation is not None:
            publication["title"] = citation
            publication["authors"] = []

        # todo: doi lookup
        res.append(publication)

    return res

def process_used_for(activity):
    """Convert an activity-dict to a string representation

    The catalog lacks a representation for a "used-for" object that
    has a title, description and URL (with just title mandatory). Best
    we can do for now is to put it into additional display. To make it
    look nicer than a standard dict rendering, we join all three
    properties into a string.

    We could make a separate additional display that used title as row
    header, but that wouldn't look great if there was *just* the
    title.

    """

    if isinstance(activity, list):
        return [process_used_for(nth_activity) for nth_activity in activity]
    if activity is None:
        return None

    text = activity.get("title", "")
    if description := activity.get("description", False):
        if type(description) is list:
            # we allowed multi-paragraph entries across columns
            description = "\n\n".join(description)
            # not that it matters for catalog display...
        text = " ".join([text, "—", description])
    if url := activity.get("url", False):
        text = " ".join([text, f"({url})"])
    return text


def add_used_for(d, activity):
    """Add activity or list of activities to a dictionary

    This takes care of adding one ("used for") or multiple ("used for
    (n)") keys to a given dictionary, so that all keys are unique and
    all activities are added.

    """
    if activity is None:
        pass
    if isinstance(activity, list):
        for n in len(activity):
            d[f"Used for ({n+1})"] = activity[n]
    else:
        d["Used for"] = activity


record = load_tabby(Path("projects/project-a/example-record/dataset@tby-crc1451v0.tsv"),
                    cpaths=[Path.cwd()/"conventions"])

cat_context = {
    "schema": "https://schema.org/",
    "bibo": "https://purl.org/ontology/bibo/",
    "obo": "https://purl.obolibrary.org/",
    "name": "schema:name",
    "title": "schema:title",
    "description": "schema:description",
    "doi": "bibo:doi",
    "version": "schema:version",
    "license": "schema:license",
    "description": "schema:description",
    "authors": "schema:author",
    "orcid": "obo:IAO_0000708",
    "email": "schema:email",
    "keywords": "schema:keywords",
    "funding": {
        "@id": "schema:funding",
        "@context": {
            "name": "schema:funder",
            "identifier": "schema:identifier",
        }
    },
    "publications": {
        "@id": "schema:creativeWork",
        "@context": {
            "doi": "schema:identifier",
            "datePublished": "schema:datePublished",
            "citation": "schema:citation",
        }
    },
    "sfbHomepage": "schema:mainEntityOfPage",
    "sfbDataController": "https://w3id.org/dpv#DataController",
    "sfbUsedFor": {
        "@id": "http://www.w3.org/ns/prov#hadUsage",
        "@context": {
            "url": "schema:url",
        }
    },
}

# crc-project, sample[organism], sample[organism-part]
# data controller


# id, version, name

expanded = jsonld.expand(record)
compacted = jsonld.compact(record, ctx=cat_context)

meta_item = {
    "type": "dataset",
    "metadata_sources": get_metadata_source(),
    "dataset_id": process_id(compacted.get("name")),
    "dataset_version": compacted.get("version"),
    "name": compacted.get(
        "title"
    ),  # note: this becomes catalog page title, so title fits better
}

meta_item["license"] = process_license(compacted.get("license"))
meta_item["description"] = compacted.get("description")
meta_item["doi"] = compacted.get("doi")
meta_item["authors"] = process_authors(compacted.get("authors"))
meta_item["keywords"] = compacted.get("keywords")
meta_item["funding"] = compacted.get("funding")
meta_item["publications"] = process_publications(compacted.get("publications"))

# top display (displayed as properties)
# max items: 5
# note: long-ish text spills out on half-screen view
# currently not using

# additional display(s)
# note: some things I don't have good schema definitions for, so I get them from "raw" record by key

meta_item["additional_display"] = [
    {
        "name": "SFB1451-Specific",
        "icon": "fa-solid fa-flask",
        "content": {
            "homepage": compacted.get("sfbHomepage"),
            "CRC project": record.get("crc-project"),
            "data controller": compacted.get("sfbDataController"),
            "sample[organism]": record.get("sample[organism]"),
            "sample[organism-part]": record.get("sample[organism-part]"),
        },
    }
]

add_used_for(
    d = meta_item["additional_display"][0]["content"],
    activity = process_used_for(compacted.get("sfbUsedFor")),
)


meta_item = {k: v for k, v in meta_item.items() if v is not None}

# display what would be added to the catalog
pprint(meta_item)

# save all the intermediate metadata for inspection
with Path("tmp").joinpath("tabby_record.json").open("w") as jsfile:
    json.dump(record, jsfile, indent=4)
with Path("tmp").joinpath("expanded.json").open("w") as jsfile:
    json.dump(expanded, jsfile, indent=4)
with Path("tmp").joinpath("compacted.json").open("w") as jsfile:
    json.dump(compacted, jsfile, indent=4)
with Path("tmp").joinpath("catalog_entry.json").open("w") as jsfile:
    json.dump(meta_item, jsfile)

# -----
# The code below is concerned with providing a ready-made catalog rendering for preview
# -----

# I have a catalog at hand to test things
catalog_dir = Path("catalog")

# I need to know id and version to clear old / set super
dsid = meta_item["dataset_id"]
dsver = meta_item["dataset_version"]

print(dsid, dsver)

# why catalog is needed: https://github.com/datalad/datalad-catalog/issues/330
catalog_validate(catalog=catalog_dir, metadata=json.dumps(meta_item))

# Remove the entry for the dataset, if present
try:
    catalog_remove(
        catalog=catalog_dir,
        dataset_id=dsid,
        dataset_version=dsver,
        reckless=True,
        on_failure="continue",
    )
except IncompleteResultsError:
    pass

# Add to the catalog
catalog_add(
    catalog=catalog_dir,
    metadata=json.dumps(meta_item),
    config_file=catalog_dir / "config.json",
)

# Set the catalog superdataset to the recently added one
# https://github.com/datalad/datalad-catalog/issues/331
catalog_set(
    catalog=catalog_dir,
    property="home",
    dataset_id=dsid,
    dataset_version=dsver,
    reckless="overwrite",
)

"""
Notes

- the "date modified" in the catalog comes from the source field
  should we maybe copy the tabby value? is it meant to be dataset or catalog modification page?
"""
