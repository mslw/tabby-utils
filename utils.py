import uuid


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
