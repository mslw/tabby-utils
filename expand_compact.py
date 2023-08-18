import json
from pathlib import Path
from pprint import pprint

from datalad_tabby.io import load_tabby
from pyld import jsonld

# record = load_tabby(Path("projects/project-a/example-record_dataset@tby-sd1.tsv"))
record = load_tabby(Path("projects/project-a/example-record/dataset@tby-sd1.tsv"))
with Path("tmp").joinpath("record.json").open("w") as jsfile:
    json.dump(record, jsfile, indent=4)

pprint(record)

# not sure if that's how I'm meant to compact, but it produces something
# compacted = jsonld.compact(record, record["@context"])
# pprint(compacted)

# pprint(jsonld.expand(record))

cat_context = {
    "schema": "https://schema.org/",
    "bibo": "https://purl.org/ontology/bibo/",
    "name": "schema:name",
    "description": "schema:description",
    "doi": "bibo:doi",
}

# # https://stackoverflow.com/a/47229455/21665899
# context_with_default = [
#     "http://schema.org/",
#     {"bibo": "https://purl.org/ontology/bibo/"}
# ]

# expand and compact https://www.cloudbees.com/blog/json-ld-building-meaningful-data-apis

expanded = jsonld.expand(record)
compacted = jsonld.compact(record, ctx=cat_context)

with Path("tmp").joinpath("re-compacted.json").open("w") as jsfile:
    json.dump(compacted, jsfile, indent=4)


#pprint(compacted)


flattened = jsonld.flatten(record)
#pprint(flattened)

#pprint(jsonld.compact(flattened, ctx=cat_context))
