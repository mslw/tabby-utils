from argparse import ArgumentParser
import json
from pathlib import Path
from pprint import pprint
import uuid
from urllib.parse import urlparse

from datalad_tabby.io import load_tabby
from datalad.api import catalog_add, catalog_remove, catalog_set, catalog_validate
from datalad_next.datasets import Dataset
from datalad_next.exceptions import IncompleteResultsError
from datalad_next.utils import get_dataset_root
from datalad_catalog.schema_utils import (
    get_metadata_item,
)

from pyld import jsonld

from queries import (
    process_ols_term,
    repr_ncbitaxon,
    repr_uberon,
)


def get_dataset_id(input, config):
    """Generate a v5 uuid"""
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


def mint_dataset_id(ds_name, project):
    """Create a deterministic id based on a custom convention

    Uses "sfb151.{project}.{ds_name}" as an input for UUID
    generation. Lowercases project. If there are multiple projects,
    uses the first one given.

    """

    dsid_input = {
        "name": ds_name,
        "project": project[0].lower() if isinstance(project, list) else project.lower(),
    }
    dsid_config = {"dataset_id_fmt": "sfb1451.{project}.{name}"}

    return get_dataset_id(dsid_input, dsid_config)


def get_tabby_subdataset_path(tabby_file_path, ds_root_path):
    """Get path of subdataset described by tabby

    Note: this is currently tuned to a single dir layout, and reports
    tabby file's parent dir as the location of described subdataset.
    If the tabby collection is located in .datalad/tabby, reports
    relative to that directory instead.

    """
    relpath = tabby_file_path.parent.relative_to(ds_root_path)
    if relpath.match(".datalad/tabby/*"):
        return relpath.relative_to(".datalad/tabby/")
    return relpath


def describe_subdataset(file_path, tabby_id, tabby_version):
    """Return catalog metadata describing subdataset relation

    If tabby file, which describes dataset T, is inside another
    dataset A, this will return a catalog metadata item describing A
    as having T as its subdataset.

    Returns None if parent dataset cannot be found.

    """

    ds_root_path = get_dataset_root(file_path)
    if ds_root_path is None:
        return None

    subds_path = get_tabby_subdataset_path(file_path, ds_root_path)

    ds = Dataset(ds_root_path)

    # Use catalog schema_utils to get base structure of metadata item
    parent_meta_item = get_metadata_item(
        item_type='dataset',
        dataset_id=ds.id,
        dataset_version=ds.repo.get_hexsha(),
        source_name="tabby",
        source_version="0.1.0",
    )
    # add sub dataset
    parent_meta_item["subdatasets"] = [
        {
            "dataset_id": tabby_id,
            "dataset_version": tabby_version,
            "dataset_path": subds_path.as_posix(),
        },
    ]

    return parent_meta_item


def process_authors(authors):
    """Convert author(s) to a list of catalog-schema authors"""
    known_keys = [
        "name",
        "email",
        "identifiers",
        "givenName",
        "familyName",
        "honorificSuffix",
    ]

    if authors is None:
        return None
    if isinstance(authors, dict):
        authors = [authors]

    result = []
    for author in authors:
        # drop not-known keys (like @type)
        d = {k: v for k, v in author.items() if k in known_keys and v is not None}
        # re-insert orcid as identifiers
        if orcid := author.get("orcid", False):
            d["identifiers"] = [
                {"name": "ORCID", "identifier": orcid},
            ]
        result.append(d)

    return result


def process_license(license):
    """Convert license to catalog-schema object

    Catalog schema expects name & url. We can reasonably expect
    schema:license to be a URL, but won't hurt checking. But what
    about the name?

    """
    if license is None:
        return None
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
    if publications is None:
        return None
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


def process_funding(funding):
    """Ensure that funding is an array"""
    return [funding] if isinstance(funding, dict) else funding


def process_keywords(keywords):
    """Ensure that keywords are an array"""
    return [keywords] if isinstance(keywords, str) else keywords


def process_arc(data_controller):
    """Convert data controller to access request contact

    Since there can only be one, uses the first data
    controller. Splits name (very naively) to satisfy catalog
    schema. Oblivious to the fact that the controller needs not be a
    person.

    """

    if data_controller is None:
        return None
    if isinstance(data_controller, list):
        data_controller = data_controller[0]

    first, _, last = data_controller.get("name", "").rpartition(" ")
    email = data_controller.get("email", "")

    return {"givenName": first, "familyName": last, "email": email}


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


def process_file(f):
    """Convert file information to catalog schema

    This gets item values (or @values, depending how they were defined
    in tabby expansion context), and does type conversion (bytesize to
    int). Returns a dictionary with catalog keys that can be read from
    tabby (does not contain type and dataset id/version).

    """

    d = {
        "path": f.get("path", {}).get("@value"),
        "contentbytesize": f.get("contentbytesize", {}).get("@value"),
        "url": f.get("url"),
    }

    if f.get("path") is None and f.get("name") is not None:
        # scoped context definition doesn't work for me as intended,
        # no idea why -- this would cover all bases
        d["path"] = f.get("name", {}).get("@value")

    if d.get("contentbytesize", False):
        # type conversion
        d["contentbytesize"] = int(d["contentbytesize"])

    return {k: v for k, v in d.items() if v is not None}


def reformat_sample_term(term):
    """Reformat tabby-dataset terms for 'sample[organism]'
    and 'sample[organism-part]' to get resolved IRI"""
    obo = "http://purl.obolibrary.org/obo/"
    if isinstance(term, list):
        return [obo + t.replace(':', '_') for t in term]
    elif isinstance(term, str):
        return obo + term.replace(':', '_')
    else:
        return None


def resolve_sfb_def(term, ctx):
    """Resolve a term definition given a context dict"""
    definition = ctx[term]
    if isinstance(definition, dict):
        definition = definition['@id']
    if definition.startswith('http'):
        return definition
    def_parts = definition.split(':')
    onto = def_parts[0]
    def_term = def_parts[1]
    return ctx[onto] + def_term


cat_context = {
    "schema": "https://schema.org/",
    "bibo": "https://purl.org/ontology/bibo/",
    "dcterms": "https://purl.org/dc/terms/",
    "nfo": "https://www.semanticdesktop.org/ontologies/2007/03/22/nfo/#",
    "obo": "https://purl.obolibrary.org/obo/",
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
        },
    },
    "publications": {
        "@id": "schema:citation",
        "@context": {
            "doi": "schema:identifier",
            "datePublished": "schema:datePublished",
            "citation": "schema:citation",
        },
    },
    "fileList": {
        "@id": "dcterms:hasPart",
        "@context": {
            "contentbytesize": "nfo:fileSize",
            "md5sum": "obo:NCIT_C171276",
            "path": "schema:name",
            "url": "schema:contentUrl",
        },
    },
    "address": "schema:PostalAddress",
    "sfbHomepage": "schema:mainEntityOfPage",
    "sfbDataController": "https://w3id.org/dpv#hasDataController",
    "sfbUsedFor": {
        "@id": "http://www.w3.org/ns/prov#hadUsage",
        "@context": {
            "url": "schema:url",
        },
    },
    "sfbProject": "schema:ResearchProject",
    "sfbSampleOrganism": "obo:UBERON_0000468",
    "sfbSamplePart": "obo:UBERON_0000475",
}

parser = ArgumentParser()
parser.add_argument("tabby_path", type=Path, help="Path to the tabby-dataset file")
parser.add_argument("--catalog", type=Path, help="Catalog to add to")
parser.add_argument("--set-as-super", action="store_true")
parser.add_argument("--remove-first", action="store_true")
args = parser.parse_args()

record = load_tabby(
    args.tabby_path,  # projects/project-a/example-record/dataset@tby-crc1451v0.tsv
    cpaths=[Path(__file__).parent / "conventions"],
)

expanded = jsonld.expand(record)
compacted = jsonld.compact(record, ctx=cat_context)

# Use catalog schema_utils to get base structure of metadata item
meta_item = get_metadata_item(
    item_type='dataset',
    dataset_id=mint_dataset_id(compacted.get("name"), record.get("crc-project")),
    dataset_version=compacted.get("version"),
    source_name="tabby",
    source_version="0.1.0",
)
# note: this becomes catalog page title, so title fits better
meta_item["name"] = compacted.get("title")
meta_item["license"] = process_license(compacted.get("license"))
meta_item["description"] = compacted.get("description")
meta_item["doi"] = compacted.get("doi")
meta_item["authors"] = process_authors(compacted.get("authors"))
meta_item["keywords"] = process_keywords(compacted.get("keywords"))
meta_item["funding"] = process_funding(compacted.get("funding"))
meta_item["publications"] = process_publications(compacted.get("publications"))
meta_item["access_request_contact"] = process_arc(compacted.get("sfbDataController"))

# top display (displayed as properties)
# max items: 5
# note: long-ish text spills out on half-screen view
# currently not using

# additional display(s)
# note: some things don't have good schema definitions for expansion,
# so we get from either compacted or (raw) record

sfb_additional_content = {
    "homepage": compacted.get("sfbHomepage"),
    "CRC project": compacted.get("sfbProject"),
    "data controller": compacted.get("sfbDataController"),
    "sample (organism)": process_ols_term(
        compacted.get("sfbSampleOrganism"),
        repr_ncbitaxon,
    ),
    "sample (organism part)": process_ols_term(
        compacted.get("sfbSamplePart"),
        repr_uberon,
    ),
}

# there can be zero, one, or more used for:
add_used_for(
    d=sfb_additional_content,
    activity=process_used_for(compacted.get("sfbUsedFor")),
)

# define an additional display tab for sfb content
meta_item["additional_display"] = [
    {
        "name": "SFB1451",
        "icon": "fa-solid fa-flask",
        "content": {k: v for k, v in sfb_additional_content.items() if v is not None},
    }
]

# add additional display definitions:
# this specifies definitions for all fields in 'additional_display' 
# except for the variables specified by users via tabby files, which
# are appended to resolvable ontologies (e.g. 'NCBITaxon:9238' or 'UBERON:0013702')
sfb_mapping = {
    "homepage": "sfbHomepage",
    "CRC project": "sfbProject",
    "data controller": "sfbDataController",
    "sample (organism)": "sfbSampleOrganism",
    "sample (organism part)": "sfbSamplePart",
    "Used for": "sfbUsedFor",
}
meta_item["additional_display_definitions"] = {
    "SFB1451": {
        "keys": {
            "homepage": resolve_sfb_def(sfb_mapping["homepage"], cat_context),
            "CRC project": resolve_sfb_def(sfb_mapping["CRC project"], cat_context),
            "data controller": {
                "self": resolve_sfb_def(sfb_mapping["data controller"], cat_context),
                "email": "https://schema.org/email",
                "name": "https://schema.org/name"
            },
            "sample (organism)": resolve_sfb_def(sfb_mapping["sample (organism)"], cat_context),
            "sample (organism part)": resolve_sfb_def(sfb_mapping["sample (organism part)"], cat_context),
            "Used for": resolve_sfb_def(sfb_mapping["Used for"], cat_context),
        },
        "values": {
            "homepage": "https://schema.org/URL",
            "CRC project": "https://schema.org/Text",
            "data controller": {
                "email": "https://schema.org/Text",
                "name": "https://schema.org/Text"
            },
            "sample (organism)": reformat_sample_term(compacted.get("sfbSampleOrganism")),
            "sample (organism part)": reformat_sample_term(compacted.get("sfbSamplePart")),
            "Used for": "https://schema.org/Text"
        }
    }
}

meta_item = {k: v for k, v in meta_item.items() if v is not None}

# display what would be added to the catalog
pprint(meta_item)

# ---
# Parent dataset
# ---

parent_meta_item = describe_subdataset(
    file_path=args.tabby_path,
    tabby_id=meta_item["dataset_id"],
    tabby_version=meta_item["dataset_version"],
)

if parent_meta_item is not None:
    pprint(parent_meta_item)


# ---
# File handling
# File handling's different, because 1 file <-> 1 metadata object
# ---

# some metadata is constant for all files
# we copy dataset id & version from (dataset-level) meta_item
file_required_meta = get_metadata_item(
    item_type='file',
    dataset_id=meta_item.get("dataset_id"),
    dataset_version=meta_item.get("dataset_version"),
    source_name="tabby",
    source_version="0.1.0",
    exclude_keys=["path"],
)

# make a list of catalog-conforming dicts
cat_file_listing = []
for file_info in compacted.get("fileList", []):
    cat_file = file_required_meta | process_file(file_info)
    cat_file_listing.append(cat_file)

# -----
# Adding to a catalog
# -----


if args.catalog is not None:
    # If a catalog path was given, use that catalog
    catalog_dir = args.catalog
else:
    # Otherwise, use a testing one in cwd
    catalog_dir = Path("catalog")

# I need to know id and version to clear old / set super
dsid = meta_item["dataset_id"]
dsver = meta_item["dataset_version"]

print(dsid, dsver)

# why catalog is needed: https://github.com/datalad/datalad-catalog/issues/330
catalog_validate(catalog=catalog_dir, metadata=json.dumps(meta_item))

# If requested and present, remove existing entries for the dataset
if args.remove_first:
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

# Add dataset metadata to the catalog
catalog_add(
    catalog=catalog_dir,
    metadata=json.dumps(meta_item),
    config_file=catalog_dir / "config.json",
)

# Link the dataset as a subdataset
if parent_meta_item is not None:
    catalog_add(
        catalog=catalog_dir,
        metadata=json.dumps(parent_meta_item),
        config_file=catalog_dir / "config.json",
    )

# Add file listing to the catalog
for cat_file in cat_file_listing:
    catalog_add(
        catalog=catalog_dir,
        metadata=json.dumps(cat_file),
        config_file=catalog_dir / "config.json",
    )

# If requested, set the catalog superdataset to the recently added one
if args.set_as_super:
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
